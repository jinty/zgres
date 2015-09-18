import os
import sys
import json
import asyncio
import argparse
import queue
from functools import partial
from collections.abc import Mapping

from pkg_resources import iter_entry_points
from kazoo.client import KazooClient

import zgres._plugin
import zgres.config
from zgres import utils

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

    def __init__(self, app):
        self.app = app
        self.zk = KazooClient(hosts=app.config['sync']['zookeeper_connection_string'])
        self.watcher = DictWatch(self.zk, app.config['sync']['zookeeper_path'], self._call_plugins)

    def _call_plugins(self, state, key, from_val, to_val):
        plugins = self.app.plugins
        if plugins.state is not None:
            databases_with_state = state_to_databases(state, True)
            plugins.state(databases_with_state)
        if plugins.conn_info is not None:
            connection_info = state_to_databases(state, False)
            if self._old_connection_info == connection_info:
                # Optimization: if the connection_info has not changed since the last event,
                # don't call our plugins again
                return
            plugins.conn_info(connection_info)
            self._old_connection_info = connection_info


class SyncApp:
    """Synchronize local machine configuration with the current postgresql state.

    Call our plugins with new state as it becomes available from the source.
    """

    def __init__(self, config):
        self.config = config
        self.plugins = zgres._plugin.get_plugins(config, 'sync', ['state', 'conn_info'], config, self)
        if self.plugins.state is None and self.plugins.conn_info is None:
            raise Exception('No plugins configured for zgres-sync')
        self.source = ZooKeeperSource(self)

#
# Command Line Scripts
#

def sync_cli(argv=sys.argv):
    parser = argparse.ArgumentParser(description="""Start synchronization daemon
This daemon connects to zookeeper an watches for changes to the database config.
It then notifies it's plugins when the state changes.

A built-in plugin is zgres-apply which writes the config out to
/var/lib/zgres/databases.json whenever there is a change and calls zgres-apply
to run arbitrary executables dropped into /var/lib/zgres/hooks.

This daemon gets run on all machines which need to know the database connection
info, that means appservers and probably database nodes if you use streaming
replication.
""")
    config = zgres.config.parse_args(parser, argv)
    # Keep a reference to the App to prevent garbage collection
    app = App(config)
    utils.run_asyncio()
    sys.exit(0)
