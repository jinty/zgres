import json
import asyncio
from asyncio import sleep
import queue
import logging
from functools import partial
from collections.abc import Mapping

import kazoo.exceptions
from kazoo.client import KazooClient, KazooState, KazooRetry

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

    def __init__(self, zk, path, callback, prefix=None, deserializer=None):
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
        if deserializer is not None:
            self._deserialize = deserializer

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
            self._child_watchers.pop(node, None) # allow our watcher to get garbage collected
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
        self._path_prefix = self.app.config['zookeeper']['path'].strip()
        if not self._path_prefix.endswith('/'):
            self._path_prefix += '/'

    @subscribe
    def start_watching(self, state, conn_info, masters, databases):
        self._storage = ZookeeperStorage(
                self.app.config['zookeeper']['connection_string'],
                self.app.config['zookeeper']['path'].strip(),
                timeout=float(self.app.config['zookeeper'].get('timeout', '10').strip()),
                )
        self._storage.dcs_connect()
        if state is not None:
            self._storage.dcs_watch_state(state)
        if conn_info is not None:
            print('CONNECT_CONN_INFO', conn_info)
            self._storage.dcs_watch_conn_info(conn_info)
        else:
            print('SKIP CONNECT_CONN_INFO', conn_info)
        if masters is not None:
            self._storage.dcs_watch_locks('master', masters)
        if databases is not None:
            self._storage.dcs_watch_database_identifiers(partial(self._notify_databases, databases))

    def _notify_databases(self, callback, state):
        callback(list(state.keys()))

def _get_clusters(in_dict):
    out_dict = {}
    for k, v in in_dict.items():
        group_name, cluster_id = k.split('-', 1)
        out_dict.setdefault(group_name, {})[cluster_id] = v
    return out_dict

class ZooKeeperDeadmanPlugin:

    _dcs_state = None

    def __init__(self, name, app):
        self.name = name
        self.app = app
        self.tick_time = app.tick_time # seconds: this should match the zookeeper server tick time (normally specified in milliseconds)
        self.logger = logging
        self._takeovers = {}
        self._kazoo_retry = KazooRetry(
                max_tries=10,
                deadline=60,
                ignore_expire=False,
                )

    def _retry(self, method, *args, **kw):
        cmd = getattr(self._storage, method)
        try:
            return self._kazoo_retry(cmd, *args, **kw)
        except kazoo.exceptions.SessionExpiredError:
            # the session has expired, we are going to restart anyway when the LOST state is set
            # however the exceptionhandler waits some time before restarting
            #
            # we want to restart immediately so call restart(0) first
            loop = asyncio.get_event_loop()
            loop.call_soon(self.app.restart, 0)
            raise

    @subscribe
    def initialize(self):
        self._loop = asyncio.get_event_loop()
        self._storage = ZookeeperStorage(
                self.app.config['zookeeper']['connection_string'],
                self.app.config['zookeeper']['path'].strip(),
                timeout=float(self.app.config['zookeeper'].get('timeout', '10').strip()),
                )
        # we start watching first to get all the state changes
        self._storage.connection.add_listener(self._session_state_handler)
        self._storage.dcs_connect()
        self._group_name = self.app.config['zookeeper']['group'].strip()
        if '/' in self._group_name or '-' in self._group_name:
            raise ValueError('cannot have - or / in the group name')

    def _session_state_handler(self, state):
        self._dcs_state = state
        self.logger.warn('zookeeper connection state: {}'.format(state))
        if state != KazooState.CONNECTED:
            self._loop.call_soon_threadsafe(self._loop.create_task, self._check_state())
        if state == KazooState.LOST:
            self._loop.call_soon_threadsafe(self.app.restart, 0)

    async def _check_state(self):
        for i in range(20):
            await sleep(1)
            if self._dcs_state == KazooState.CONNECTED:
                return
        # we could not re-connect within 4 seconds,
        # so we assume all is lost and we should restart
        self._loop.call_soon(self.app.restart, 0)

    @subscribe
    def dcs_set_database_identifier(self, database_id):
        return self._retry('dcs_set_database_identifier', self._group_name, database_id)

    @subscribe
    def dcs_get_database_identifier(self):
        return self._retry('dcs_get_database_identifier', self._group_name)

    @subscribe
    def dcs_set_timeline(self, timeline):
        return self._retry('dcs_set_timeline', self._group_name, timeline)

    @subscribe
    def dcs_get_timeline(self):
        return self._retry('dcs_get_timeline', self._group_name)

    def _only_my_cluster_filter(self, callback):
        def f(value):
            callback(value.get(self._group_name, {}))
        return f

    @subscribe
    def dcs_watch(self, master_lock, state, conn_info):
        if master_lock is not None:
            self._storage.dcs_watch_lock('master', self._group_name, master_lock)
        if state is not None:
            self._storage.dcs_watch_state(
                    self._only_my_cluster_filter(state),
                    self._group_name)
        if conn_info is not None:
            self._storage.dcs_watch_conn_info(
                    self._only_my_cluster_filter(conn_info),
                    self._group_name)

    @subscribe
    def dcs_get_lock_owner(self, name):
        return self._retry('dcs_get_lock_owner', self._group_name, name)

    @subscribe
    def dcs_lock(self, name):
        result = self._retry('dcs_lock', 
                self._group_name,
                name,
                self.app.my_id)
        if result in ('locked', 'owned'):
            return True
        elif result == 'broken':
            self._log_takeover('lock/{}/{}'.format(self._group_name, self.app.my_id))
            return True
        elif result == 'failed':
            return False
        raise AssertionError(result)

    @subscribe
    def dcs_unlock(self, name):
        self._retry('dcs_unlock', self._group_name, name, self.app.my_id)

    def _log_takeover(self, path):
        if self._takeovers.get(path, False):
            # hmm, I have taken over before, this is NOT good
            # maybe 2 of me are running
            self.logger.error('Taking over again: {}\n'
                    'This should not happen, check that you do not '
                    'have 2 nodes with the same id running'.format(path))
        else:
            # first time I am taking over, probably normal operation after a restart
            self.logger.info('Taking over {}'.format(path))
        self._takeovers[path] = True

    @subscribe
    def dcs_set_conn_info(self, conn_info):
        how = self._retry('dcs_set_conn_info', self._group_name, self.app.my_id, conn_info)
        if how == 'takeover':
            self._log_takeover('conn/{}/{}'.format(self._group_name, self.app.my_id))

    @subscribe
    def dcs_set_state(self, state):
        how = self._retry('dcs_set_state', self._group_name, self.app.my_id, state)
        if how == 'takeover':
            self._log_takeover('state/{}/{}'.format(self._group_name, self.app.my_id))

    @subscribe
    def dcs_list_conn_info(self):
        return self._retry('dcs_list_conn_info', group=self._group_name)

    @subscribe
    def dcs_list_state(self):
        return self._retry('dcs_list_state', group=self._group_name)

    @subscribe
    def dcs_delete_conn_info(self):
        self._retry('dcs_delete_conn_info',
                self._group_name,
                self.app.my_id)

    @subscribe
    def dcs_disconnect(self):
        self._storage.connection.remove_listener(self._session_state_handler)
        self._storage.dcs_disconnect()


class ZookeeperStorage:
    """A low level storage object.

    Manages and publishes the zookeeper connection.

    Manages the database "schema" and allows access to multiple "groups"
    database servers, each representing one logical cluster.
    """

    _zk = None

    def __init__(self, connection_string, path, timeout=10.0):
        self._connection_string = connection_string
        self._path_prefix = path
        self._timeout = timeout
        if not self._path_prefix.endswith('/'):
            self._path_prefix += '/'
        self._watchers = {}
        self._loop = asyncio.get_event_loop()

    @property
    def connection(self):
        if self._zk is None:
            self._zk = KazooClient(
                    hosts=self._connection_string,
                    timeout=self._timeout)
        return self._zk

    def dcs_connect(self):
        self.connection.start()

    def dcs_disconnect(self):
        self._zk.stop()
        self._zk = None

    def _dict_watcher(self, group, what, callback):
        def hook(state, key, from_val, to_val):
            callback(_get_clusters(state))
        path = self._folder_path(what)
        prefix = group and group + '-' or group
        try:
            watch = DictWatch(self._zk, path, hook, prefix=prefix)
        except kazoo.exceptions.NoNodeError:
            self._zk.create(path, makepath=True)
            return self._dict_watcher(group, what, callback)
        self._watchers[id(watch)] = watch
        return watch

    def _listen_connection(self, state):
        self._connection_state_changes.append(state)
        self._loop.call_soon_threadsafe(self._consume_connection_state_changes)

    def dcs_watch_conn_info(self, callback, group=None):
        self._dict_watcher(group, 'conn', callback)

    def dcs_watch_state(self, callback, group=None):
        self._dict_watcher(group, 'state', callback)

    def _folder_path(self, folder):
        return self._path_prefix + folder

    def _path(self, group, folder, key):
        return self._path_prefix + folder + '/' + group + '-' + key

    def _get_static(self, group, key):
        path = self._path(group, 'static', key)
        try:
            data, stat = self._zk.get(path)
        except kazoo.exceptions.NoNodeError:
            return None
        return data

    def _set_static(self, group, key, data, overwrite=False):
        path = self._path(group, 'static', key)
        try:
            self._zk.create(path, data, makepath=True)
        except kazoo.exceptions.NodeExistsError:
            if overwrite:
                self._zk.set(path, data)
                return True
            return False
        return True

    def dcs_get_timeline(self, group):
        data = self._get_static(group, 'timeline')
        if data is None:
            data = b'0'
        return int(data.decode('ascii'))

    def dcs_set_timeline(self, group, timeline):
        assert isinstance(timeline, int)
        existing = self.dcs_get_timeline(group)
        if existing > timeline:
            raise ValueError('Timelines can only increase.')
        timeline = str(timeline).encode('ascii')
        self._set_static(group, 'timeline', timeline, overwrite=True)

    def dcs_set_database_identifier(self, group, database_id):
        database_id = database_id.encode('ascii')
        return self._set_static(group, 'database_identifier', database_id)

    def dcs_get_database_identifier(self, group):
        data = self._get_static(group, 'database_identifier')
        if data is not None:
            data = data.decode('ascii')
        return data

    def dcs_get_lock_owner(self, group, name):
        path = self._path(group, 'lock', name)
        try:
            existing_data, stat = self._zk.get(path)
        except kazoo.exceptions.NoNodeError:
            return None
        return existing_data.decode('utf-8')

    def dcs_unlock(self, group, name, owner):
        existing_owner = self.dcs_get_lock_owner(group, name)
        if existing_owner == owner:
            path = self._path(group, 'lock', name)
            self._zk.delete(path)

    def dcs_lock(self, group, name, owner):
        data = owner.encode('utf-8')
        path = self._path(group, 'lock', name)
        try:
            self._zk.create(path, data, ephemeral=True, makepath=True)
            return 'locked'
        except kazoo.exceptions.NodeExistsError:
            pass
        # lock exists, do we have it, can we break it?
        try:
            existing_data, stat = self._zk.get(path)
        except kazoo.exceptions.NoNodeError:
            # lock broke while we were looking at it
            # try get it again
            return self.dcs_lock(group, name, owner)
        if stat.owner_session_id == self._zk.client_id[0]:
            # we already own the lock
            return 'owned'
        elif data == existing_data:
            # it is our log, perhaps I am restarting. of there are 2 of me running!
            try:
                self._zk.delete(path, version=stat.version)
            except (kazoo.exceptions.NoNodeError, kazoo.exceptions.BadVersionError):
                # lock broke while we were looking at it
                pass
            # try get the lock again
            result = self.dcs_lock(group, name, owner)
            if result == 'locked':
                return 'broken'
            return result
        return 'failed'

    def dcs_watch_lock(self, name, group, callback):
        loop = asyncio.get_event_loop()
        def handler(data, stat, event):
            if data is not None:
                data = data.decode('utf-8')
            callback(data)
        path = self._path(group, 'lock', name)
        w = self._zk.DataWatch(path, partial(loop.call_soon_threadsafe, handler))
        self._watchers[id(w)] = w

    def dcs_get_database_identifiers(self):
        wanted_info_name = 'database_identifier'
        dirpath = self._folder_path('static')
        try:
            children = self._zk.get_children(dirpath)
        except kazoo.exceptions.NoNodeError:
            return {}
        result = {}
        for name in children:
            owner, info_name = name.split('-', 1)
            if wanted_info_name != info_name:
                continue
            try:
                data, state = self._zk.get(dirpath + '/' + name)
            except kazoo.exceptions.NoNodeError:
                continue
            state = json.loads(data.decode('ascii'))
            result[owner] = state
        return result

    def dcs_watch_database_identifiers(self, callback):
        name = 'database_identifier'
        def handler(state, key, from_val, to_val):
            # this is probably more complex than it needs to be!
            c_state = _get_clusters(state)
            new_state = {}
            for k, v in c_state.items():
                ours = v.get(name, None)
                if ours is not None:
                    new_state[k] = ours
            callback(new_state)
        dirpath = self._folder_path('static')
        watch = DictWatch(
                self._zk,
                dirpath,
                handler,
                deserializer=lambda data: data.decode('utf-8'))
        self._watchers[id(watch)] = watch

    def dcs_watch_locks(self, name, callback):
        def handler(state, key, from_val, to_val):
            # this is probably more complex than it needs to be!
            c_state = _get_clusters(state)
            new_state = {}
            for k, v in c_state.items():
                ours = v.get(name, None)
                if ours is not None:
                    new_state[k] = ours
            callback(new_state)
        dirpath = self._folder_path('lock')
        watch = DictWatch(
                self._zk,
                dirpath,
                handler,
                deserializer=lambda data: data.decode('utf-8'))
        self._watchers[id(watch)] = watch

    def _set_info(self, group, type, owner, data):
        path = self._path(group, type, owner)
        data = json.dumps(data)
        data = data.encode('ascii')
        try:
            stat = self._zk.set(path, data)
            how = 'existing'
        except kazoo.exceptions.NoNodeError:
            how = 'create'
            stat = None
        if stat is not None and stat.owner_session_id != self._zk.client_id[0]:
            self._zk.delete(path)
            how = 'takeover'
            stat = None
        if stat is None:
            self._zk.create(path, data, ephemeral=True, makepath=True)
        return how

    def dcs_set_conn_info(self, group, owner, data):
        return self._set_info(group, 'conn', owner, data)

    def dcs_set_state(self, group, owner, data):
        return self._set_info(group, 'state', owner, data)

    def _get_all_info(self, group, type):
        dirpath = self._folder_path(type)
        try:
            children = self._zk.get_children(dirpath)
        except kazoo.exceptions.NoNodeError:
            return iter([])
        for name in children:
            this_group, owner = name.split('-', 1)
            if group is not None and this_group != group:
                continue
            data, state = self._zk.get(dirpath + '/' + name)
            state = json.loads(data.decode('ascii'))
            yield owner, state

    def dcs_list_conn_info(self, group=None):
        return list(self._get_all_info(group, 'conn'))

    def dcs_list_state(self, group=None):
        return list(self._get_all_info(group, 'state'))

    def dcs_delete_conn_info(self, group, owner):
        path = self._path(group, 'conn', owner)
        try:
            self._zk.delete(path)
        except kazoo.exceptions.NoNodeError:
            pass
