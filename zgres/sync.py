import os
import sys
import json
import asyncio
import argparse
from functools import partial

from pkg_resources import iter_entry_points
from kazoo.client import KazooClient

import zgres._plugin
import zgres.config
from zgres import utils

def _watch_node(zk, zookeeper_base_path, node, state, notify):
    """Watch a single node in zookeeper for data changes."""
    if not zookeeper_base_path.endswith('/'):
        zookeeper_base_path += '/'
    child_path = zookeeper_base_path + node
    def watch_node(data, stat, event):
        if data is None:
            state.pop(node, None)
        else:
            data = json.loads(data.decode('utf-8'))
            state[node] = data
        notify()
    loop = asyncio.get_event_loop()
    watch_node = partial(loop.call_soon_threadsafe, watch_node) # execute in main thread
    return zk.DataWatch(child_path, watch_node)

def watch_cluster_groups(zk, zookeeper_base_path, plugins):
    """Watch for changes in the list of children.

    This method returns a "state" dictionary which is kept in-sync with a
    directory in zookeeper. i.e. the "files" in zookeeper are a key in the dict
    and the data in the file is the value. Data is assumed to be in the JSON
    format and is decoded on read.
    """
    state = {}
    notify = make_notify(state, plugins)    
    child_watchers = {} # Do we need locking on these? it's all idempotent, maybe not
    def watch(children):
        to_add = set(children) - set(child_watchers)
        to_delete = set(child_watchers) - set(children)
        for i in to_delete:
            state.pop(i, None)
            del child_watchers[i]
        for node in to_add:
            child_watchers[node] = _watch_node(zk, zookeeper_base_path, node, state, notify)
        if to_delete:
            # _watch_node will call notify() in case of to_add
            notify()
    loop = asyncio.get_event_loop()
    watch = partial(loop.call_soon_threadsafe, watch) # execute in main thread
    return state, zk.ChildrenWatch(zookeeper_base_path, watch)

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

def make_notify(state, plugins):
    old_connection_info = None
    def notify():
        nonlocal old_connection_info
        if plugins.state is not None:
            databases_with_state = state_to_databases(state, True)
            plugins.state(databases_with_state)
        if plugins.conn_info is not None:
            connection_info = state_to_databases(state, False)
            if old_connection_info == connection_info:
                # Optimization: if the connection_info has not changed since the last event,
                # don't call our plugins again
                return
            plugins.conn_info(connection_info)
            old_connection_info = connection_info
    return notify

def _sync(config, zk):
    """Synchronize local machine configuration with zookeeper.

    Connect to zookeeper and call our plugins with new state as it becomes available.
    """
    plugins = zgres._plugin.get_plugins(config, 'sync', ['state', 'conn_info'], config, zk)
    if plugins.state is None and plugins.conn_info is None:
        raise Exception('No plugins configured for zgres-sync')
    return watch_cluster_groups(zk, config['sync']['zookeeper_path'], plugins)

#
# Command Line Scripts
#

def sync_cli(argv=sys.argv):
    parser = argparse.ArgumentParser(description="""Start synchronization daemon
This daemon connects to zookeeper an watches for changes to the database config.
It then writes the config out to /var/lib/zgres/databases.json whenever there is a change
and calls zgres-apply.

This daemon gets run on all machines which need to know the database connection
info, that means appservers and probably database nodes if you use streaming
replication.
""")
    config = zgres.config.parse_args(parser, argv)
    zk = KazooClient(hosts=config['sync']['zookeeper_connection_string'])
    # NOTE: I think we need to keep the reference to the watcher
    # to prevent it from being garbage collected
    watcher = _sync(config, zk)
    utils.run_asyncio()
    sys.exit(0)
