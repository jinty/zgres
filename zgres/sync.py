import os
import sys
import json

from pkg_resources import iter_entry_points
from kazoo.client import KazooClient

import zgres._plugin
import zgres.config

def _watch_node(zookeeper_base_path, node, state, notify):
    """Watch a single node in zookeeper for data changes."""
    child_path = zookeeper_base_path + node
    def watch_node(data, state):
        try:
            if data is None:
                state.pop(node, None)
            else:
                data = json.loads(data)
                state[node] = data
            notify()
        except:
            logging.exception()
            notify(fatal_error=True)
            raise
    return zk.DataWatch(child_path, watch_node)

def watch_cluster_groups(zk, zookeeper_base_path, notify):
    """Watch for changes in the list of children.

    This method returns a "state" dictionary which is kept in-sync with a
    directory in zookeeper. i.e. the "files" in zookeeper are a key in the dict
    and the data in the file is the value. Data is assumed to be in the JSON
    format and is decoded on read.

    notify() will be called without argument if the state dictionary is
    modified.  On any error, notify(fatal_error=True) will be called. This
    should abort the entire process noisly and get restarted by whatever system
    watches over this daemon.
    """
    state = {}
    child_watchers = {} # Do we need locking on these? it's all idempotent, maybe not
    def watch(children, event):
        try:
            to_add = set(children) - set(child_watchers)
            to_delete = set(child_watchers) - set(children)
            for i in to_delete:
                state.pop(i, None)
                del child_watchers[i]
            for node in to_add:
                child_watchers[node] = _watch_node(zookeeper_base_path, node, state, notify)
            if to_delete:
                # _watch_node will call notify() in case of to_add
                notify()
        except:
            logging.exception()
            notify(fatal_error=True)
            raise
    return state, zk.ChildrenWatch(zookeeper_base_path, watch, send_event=True)

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
        if node.startswith('state-') and get_state:
            _, ip4 = node.split('-', 1)
            database = databases.setdefault(v['cluster_name'], dict(nodes={}))
            assert ip4 not in database['nodes']
            database['nodes'][ip4] = v
        if node.startswith('conn-') and not get_state:
            _, ip4 = node.split('-', 1)
            database = databases.setdefault(v['cluster_name'], dict(nodes={}))
            assert ip4 not in database['nodes']
            database['nodes'][ip4] = v
        if node.startswith('master-'):
            _, cluster_name = node.split('-', 1)
            cluster = databases.setdefault(cluster_name, dict(nodes={}))
            cluster['master'] = v
    return databases

def _sync(config):
    """Synchronize local machine configuration with zookeeper.

    Connect to zookeeper and call our plugins with new state as it becomes available.
    """
    state = {}
    zk = KazooClient(hosts=config['global']['zookeeper_connection_string'])
    connection_info_plugins = zgres._plugin.get_configured_plugins(config, 'zgres.conn')
    state_plugins = zgres._plugin.get_configured_plugins(config, 'zgres.state')
    if not state_plugins and not connection_info_plugins:
        raise Exception('No plugins configured for zgres-sync')
    notifiy_queue = queue.Queue()
    def notify_main_thread(fatal_error=False):
        # poor man's async framework
        notifiy_queue.put(fatal_error)
    state, watcher = watch_cluster_group(zk, config['sync']['zookeeper_path'], notify_main_thread)
    old_connection_info = None
    while True:
        fatal_error = notifiy_queue.get() # blocks till we get some new data from zookeeper
        if fatal_error:
            raise Exception('Got a fatal error, shutting down. Hopefully some info in the logs above! and hope systemd restarts us!')
        if state_plugins:
            databases_with_state = state_to_databases(state, True)
            zgres._plugin.call_plugins(state_plugins, databases_with_state)
        if connection_info_plugins:
            connection_info = state_to_databases(state, False)
            if old_connection_info == connection_info:
                # Optimization: if the connection_info has not changed since the last event,
                # don't call our plugins again
                continue
            zgres._plugin.call_plugins(connection_info_plugins, connection_info)
            old_connection_info = connection_info

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
    parser.add_argument('--config-file',
            dest='config_file',
            default='/etc/zgres/example.ini',
            help='sum the integers (default: find the max)')
    config = zgres.config.parse_args(parser, argv)
    sys.exit(_sync(config))
