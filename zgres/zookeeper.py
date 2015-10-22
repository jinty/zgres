import json
import asyncio
import queue
from functools import partial
from collections.abc import Mapping

import kazoo.exceptions
from kazoo.client import KazooClient, KazooState

from .plugin import subscribe

_missing = object()

def state_to_databases(state, get_state):
    """Convert the state to a dict of connectable database clusters.

    The dict is:
        {"databases": {
            "databaseA": {
                "master": "10.0.0.19",
                "nodes": {
                    "10.0.0.9":
                        {
                            ...extra node data...
                            },
                    "10.0.0.10":
                        {
                            ...extra node data...
                            },
                    "10.0.0.99":
                        {
                            ...extra node data...
                            }}},
            "databaseB": {
                "master": "10.0.0.19",
                "nodes": {
                    "10.0.0.19":
                        {
                            ...extra node data...
                            },
                    "10.0.0.20":
                        {
                            ...extra node data...
                            }}},
                            }}

    if get_state=True, then node data will be the contents of the state-
    znodes. If get_state=False, the node data will be the contents of the
    conn- znodes.
    """
    databases = {}
    for node, v in state.items():
        parts = node.split('_', 1)
        if len(parts) == 1:
            continue
        cluster_name, node = parts
        if node.startswith('state_') and get_state:
            _, ip4 = node.split('_', 1)
            database = databases.setdefault(cluster_name, dict(nodes={}))
            assert ip4 not in database['nodes']
            database['nodes'][ip4] = v
        if node.startswith('conn_') and not get_state:
            _, ip4 = node.split('_', 1)
            database = databases.setdefault(cluster_name, dict(nodes={}))
            assert ip4 not in database['nodes']
            database['nodes'][ip4] = v
        if node == 'master':
            cluster = databases.setdefault(cluster_name, dict(nodes={}))
            cluster['master'] = v
    return databases

class DictWatch(Mapping):
    """A mapping which reflects the content of a path in ZooKeeper.

    For every znode in the path, there will exist a key (znode name) and value
    (deserialized node content) in the mapping. To be notified if the mapping
    changes, you can pass a callback function, this will be called on change.

    The callback is called with 4 arguments: (mapping, key, from_value, to_value)
        * mapping is the DictWatch object
        * key is the dictionary key
        * to_value is the value which is being set
        * from_value is the value which was there previously

    The callack WILL be called in the main thread and the order of events from
    zookeeper will be maintained.

    On add from_value is DictWatch.MISSING, on delete, to value will be
    DictWatch.MISSING.

    The implementation of this is that kazoo-fired events will be put on a
    threadsafe queue and will be processed later (in order) in the asyncio main
    thread.
    """

    MISSING = object()

    def __init__(self, zk, path, callback, prefix=None):
        self._zk = zk
        self._callback = callback
        self._state = {}
        if not path.endswith('/'):
            path += '/'
        self._path = path
        self._child_watchers = {}
        self._loop = asyncio.get_event_loop()
        self._zk_event_queue = queue.Queue()
        self._prefix = prefix
        self._watch()

    def _watch(self):
        """Start watching."""
        watch = partial(self._queue_event, '_children_changed')
        self._child_watcher = self._zk.ChildrenWatch(self._path, watch)

    def __getitem__(self, key):
        return self._state[key]

    def __iter__(self):
        return iter(self._state)

    def __len__(self):
        return len(self._state)

    def _deserialize(self, data):
        data = data.decode('ascii')
        return json.loads(data)

    def _queue_event(self, event_name, *args, **kw):
        # Note: this runs in the kazoo thread, hence we use
        # a threadsafe queue
        self._zk_event_queue.put((event_name, args, kw))
        self._loop.call_soon_threadsafe(self._consume_queue)

    def _consume_queue(self):
        while True:
            try:
                event_name, args, kw = self._zk_event_queue.get(block=False)
            except queue.Empty:
                return
            getattr(self, event_name)(*args, **kw)

    def _watch_node(self, node):
        child_path = self._path + node
        watch = partial(self._queue_event, '_node_changed', node)
        return self._zk.DataWatch(child_path, watch)

    def _node_changed(self, node, data, stat, event):
        """Watch a single node in zookeeper for data changes."""
        old_val = self._state.pop(node, self.MISSING)
        if data is None:
            new_val = self.MISSING
            del self._child_watchers[node] # allow our watcher to get garbage collected
        else:
            new_val = self._deserialize(data)
            self._state[node] = new_val
        if old_val is self.MISSING and new_val is self.MISSING:
            # we re-deleted an already deleted node
            return
        if old_val == new_val:
            # no change
            return
        self._callback(self, node, old_val, new_val)

    def _children_changed(self, children):
        to_add = set(children) - set(self._child_watchers)
        for node in to_add:
            if self._prefix is not None:
                if not node.startswith(self._prefix):
                    continue
            self._child_watchers[node] = self._watch_node(node)


class ZooKeeperSource:

    _old_connection_info = None

    def __init__(self, name, app):
        self.app = app

    @subscribe
    def start_watching(self):
        self.zk = KazooClient(hosts=self.app.config['sync']['zookeeper']['connection_string'])
        self.zk.start()
        self.watcher = DictWatch(self.zk, self.app.config['sync']['zookeeper']['path'], self._notify_app_of_changes)

    def _notify_app_of_changes(self, state, key, from_val, to_val):
        app = self.app
        if app.state is not None:
            databases_with_state = state_to_databases(state, True)
            app.state(databases_with_state)
        if app.conn_info is not None:
            connection_info = state_to_databases(state, False)
            if self._old_connection_info == connection_info:
                # Optimization: if the connection_info has not changed since the last event,
                # don't call our app again
                return
            app.conn_info(connection_info)
            self._old_connection_info = connection_info

def _get_clusters(in_dict):
    out_dict = {}
    for k, v in in_dict.items():
        group_name, cluster_id = k.split('-')
        out_dict.setdefault(group_name, {})[cluster_id] = v
    return out_dict

class ZooKeeperDeadmanPlugin:

    def __init__(self, name, app):
        self.name = name
        self.app = app
        self._monitors = {}
        self.tick_time = app.tick_time # seconds: this should match the zookeeper server tick time (normally specified in milliseconds)

    def _path(self, type, name=_missing):
        if name is _missing:
            name = self.app.my_id
        if name:
            return self._path_prefix + type + '/' + self._group_name + '-' + name
        else:
            return self._path_prefix + type

    def _lock_path(self, name):
        return self._path('lock', name)

    @subscribe
    def initialize(self):
        self._loop = asyncio.get_event_loop()
        self._zk = KazooClient(hosts=self.app.config['zookeeper']['connection_string'])
        self._zk.add_listener(self._session_state_threadsafe)
        self._zk.start()
        self._path_prefix = self.app.config['zookeeper']['path'].strip()
        if not self._path_prefix.endswith('/'):
            self._path_prefix += '/'
        self._group_name = self.app.config['zookeeper']['group'].strip()
        if '/' in self._group_name or '-' in self._group_name:
            raise ValueError('cannot have - or / in the group name')

    def _session_state_threadsafe(self, state):
        self._loop.call_soon_threadsafe(self._loop.create_task, self._session_state(state))

    async def _session_state(self, state):
        unhealthy_key = '{}.no_zookeeper_connection'.format(self.name)
        # runs in separate thread
        if state == KazooState.SUSPENDED:
            # we wait for the tick time before taking action to see if
            # our session gets re-established
            await asyncio.sleep(self.tick_time)
            # we have to assume we are irretrevably lost, minimum session
            # timeout in zookeeper is 2 * tick time so stop postgresql now
            # and let a failover happen
            if self._zk.state != KazooState.CONNECTED:
                self.app.unhealthy(unhealthy_key, 'No connection to zookeeper: {}'.format(self._zk.state), can_be_replica=True)
        elif state == KazooState.LOST:
            self.app.restart(10)
            raise AssertionError('We should never get here')
        else:
            self.app.healthy(unhealthy_key)

    def _get_static(self, key):
        path = self._path('static', key)
        try:
            data, stat = self._zk.get(path)
        except kazoo.exceptions.NoNodeError:
            return None
        return data
    
    def _set_static(self, key, data, overwrite=False):
        path = self._path('static', key)
        try:
            self._zk.create(path, data, makepath=True)
        except kazoo.exceptions.NodeExistsError:
            if overwrite:
                self._zk.set(path, data)
                return True
            return False
        return True

    @subscribe
    def dcs_set_database_identifier(self, database_id):
        database_id = database_id.encode('ascii')
        return self._set_static('database_identifier', database_id)

    @subscribe
    def dcs_get_database_identifier(self):
        data = self._get_static('database_identifier')
        if data is not None:
            data = data.decode('ascii')
        return data

    @subscribe
    def dcs_set_timeline(self, timeline):
        assert isinstance(timeline, int)
        existing = self.dcs_get_timeline()
        if existing > timeline:
            raise ValueError('Timelines can only increase.')
        timeline = str(timeline).encode('ascii')
        self._set_static('timeline', timeline, overwrite=True)

    @subscribe
    def dcs_get_timeline(self):
        data = self._get_static('timeline')
        if data is None:
            data = b'0'
        return int(data.decode('ascii'))

    def _notify_state(self, state, key, from_val, to_val):
        self.app.state(_get_clusters(state).get(self._group_name, {}))

    def _notify_conn(self, state, key, from_val, to_val):
        self.app.conn_info(_get_clusters(state).get(self._group_name, {}))

    def _dict_watcher(self, what):
        hook = getattr(self, '_notify_' + what)
        path = self._path(what, name=None)
        prefix = self._group_name
        try:
            watch = DictWatch(self._zk, path, hook, prefix=prefix)
        except kazoo.exceptions.NoNodeError:
            self._zk.create(path, makepath=True)
            return self._dict_watcher(what)
        return watch

    @subscribe
    def dcs_watch(self, state=None, conn_info=None):
        path = self._lock_path('master')
        self._monitors['master_lock_watch'] = self._zk.DataWatch(path, self._master_lock_changes)
        if state:
            self._state_watcher = self._dict_watcher('state')
        if conn_info:
            self._state_watcher = self._dict_watcher('conn')

    def _master_lock_changes(self, data, stat, event):
        if data is not None:
            data = data.decode('utf-8')
        self._loop.call_soon_threadsafe(self.app.master_lock_changed, data)

    @subscribe
    def dcs_lock(self, name):
        path = self._lock_path(name)
        try:
            self._zk.create(path, self.app.my_id.encode('utf-8'), ephemeral=True, makepath=True)
            return True
        except kazoo.exceptions.NodeExistsError:
            pass
        try:
            owner, stat = self._zk.get(self._lock_path(name))
        except kazoo.exceptions.NoNodeError:
            return False
        if stat.owner_session_id == self._zk.client_id[0]:
            return True
        return False

    @subscribe
    def dcs_unlock(self, name):
        owner = self._get_lock_owner(name)
        if owner == self.app.my_id:
            self._zk.delete(self._lock_path(name))

    def _get_lock_owner(self, name):
        try:
            owner, stat = self._zk.get(self._lock_path(name))
        except kazoo.exceptions.NoNodeError:
            return None
        return owner.decode('utf-8')

    def _set_info(self, type, data):
        data = json.dumps(data)
        data = data.encode('ascii')
        try:
            self._zk.set(self._path(type), data)
        except kazoo.exceptions.NoNodeError:
            self._zk.create(self._path(type), data, ephemeral=True, makepath=True)

    @subscribe
    def dcs_set_conn_info(self, data):
        return self._set_info('conn', data)

    @subscribe
    def dcs_set_state(self, data):
        return self._set_info('state', data)

    def _get_all_info(self, type):
        dirpath = self._path_prefix + type
        try:
            children = self._zk.get_children(dirpath, include_data=True)
        except kazoo.exceptions.NoNodeError:
            return iter([])
        for name, data in children:
            if not name.startswith(self._group_name + '-'):
                continue
            data = data['data']
            state = json.loads(data.decode('ascii')) 
            yield name[len(self._group_name + '-'):], state
    
    @subscribe
    def dcs_get_all_conn(self):
        return self._get_all_info('conn')

    @subscribe
    def dcs_get_all_state(self):
        return self._get_all_info('state')

    def _delete_info(self, type):
        try:
            self._zk.delete(self._path(type))
        except kazoo.exceptions.NoNodeError:
            pass
    
    @subscribe
    def dcs_delete_conn(self):
        return self._delete_info('conn')

    @subscribe
    def dcs_delete_state(self):
        return self._get_all_info('state')

    @subscribe
    def dcs_disconnect(self):
        # for testing only
        self._zk.remove_listener(self._session_state_threadsafe)
        self._zk.stop()

