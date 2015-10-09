import json
import asyncio
import queue
from functools import partial
from collections.abc import Mapping

import kazoo.exceptions
from kazoo.client import KazooClient

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

    On add from_value is DictWatch.MISSING, on delete, to value will be
    DictWatch.MISSING.

    The implementation of this is that kazoo-fired events will be put on a
    threadsafe queue and will be processed later (in order) in the asyncio main
    thread.
    """

    MISSING = object()

    def __init__(self, zk, path, callback):
        self._zk = zk
        self._callback = callback
        self._state = {}
        if not path.endswith('/'):
            path += '/'
        self._path = path
        self._child_watchers = {}
        self._loop = asyncio.get_event_loop()
        self._zk_event_queue = queue.Queue()
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
            self._child_watchers[node] = self._watch_node(node)


class ZooKeeperSource:

    _old_connection_info = None

    def __init__(self, name, app):
        self.app = app

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

class ZooKeeperDeadmanPlugin:

    def __init__(self, name, app):
        self.app = app

    def _path(self, type, name=None):
        if name is None:
            name = self.app.my_id
        return self._path_prefix + type + '/' + self._group_name + '-' + name

    def _db_id_path(self):
        return self._path('static', 'db-id')

    def _lock_path(self, name):
        return self._path('lock', name)

    def initialize(self):
        self._zk = KazooClient(hosts=self.app.config['zookeeper']['connection_string'])
        self._zk.start()
        self._path_prefix = self.app.config['zookeeper']['path'].strip()
        if not self._path_prefix.endswith('/'):
            self._path_prefix += '/'
        self._group_name = self.app.config['zookeeper']['group'].strip()
        assert '/' not in self._group_name

    def dcs_set_database_identifier(self, database_id):
        database_id = database_id.encode('ascii')
        try:
            self._zk.create(self._db_id_path(), database_id, makepath=True)
        except kazoo.exceptions.NodeExistsError:
            return False
        return True

    def dcs_get_database_identifier(self):
        try:
            dbid, stat = self._zk.get(self._db_id_path())
        except kazoo.exceptions.NoNodeError:
            return None
        return dbid.decode('ascii')

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
        if stat.owner_session_id == self._zk.session_id:
            return True
        return False

    def dcs_unlock(self, name):
        owner = self.dcs_get_lock_owner(name)
        if owner == self.app.my_id:
            self._zk.delete(self._lock_path(name))

    def dcs_get_lock_owner(self, name):
        try:
            owner, stat = self._zk.get(self._lock_path(name))
        except kazoo.exceptions.NoNodeError:
            return None
        return owner.decode('utf-8')

    def dcs_set_info(self, type, data):
        data = json.dumps(data)
        data = data.encode('ascii')
        try:
            self._zk.set(self._path(type), data)
        except kazoo.exceptions.NoNodeError:
            self._zk.create(self._path(type), data, ephemeral=True, makepath=True)

    def dcs_get_info(self, type):
        try:
            data, stat = self._zk.get(self._path(type))
        except kazoo.exceptions.NoNodeError:
            return None
        return json.loads(data.decode('ascii'))

    def dcs_delete_info(self, type):
        try:
            self._zk.delete(self._path(type))
        except kazoo.exceptions.NoNodeError:
            pass

    def _disconnect(self):
        # for testing only
        self._zk.stop()

