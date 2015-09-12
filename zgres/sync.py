import os
import sys
import json

from kazoo.client import KazooClient

from zgres import apply

def _watch_node(zookeeper_base_path, node, state, notify):
    """Watch a single node in zookeeper for data changes."""
    child_path = zookeeper_base_path + node
    def watch_node(data, stat):
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

def watch_cluster_group(zk, zookeeper_base_path, notify):
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

def state_to_databases(state):
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
    """
    databases = {}
    for ip4, v in state.items():
        if ip4.startswith('master-'):
            continue
        database = databases.setdefault(v['cluster_name'], dict(nodes={}))
        assert ip4 not in database['nodes']
        database['nodes'] = v
    for node, v in state.items():
        if not node.startswith('master-'):
            continue
        _, cluster_name = node.split('-', 1)
        cluster = databases.get(cluster_name)
        if cluster is not None:
            cluster['master'] = v
    return databases

def _sync(config):
    """Synchronize local machine configuration with zookeeper.
    
    - Connect to zookeeper using zookeeper_connection_string from environment.json
    - Watch the ZK directory configured in environment.json (i.e. the value of zgres_databases_path)
    - Write out /var/lib/zgres/databases.json whenever this changes
    - call zgres-apply on any changes in databases.json
    """
    config = config['environment.json']
    state = {}
    zk = KazooClient(hosts=config['zookeeper_connection_string'])
    notifiy_queue = queue.Queue()
    def notify_main_thread(fatal_error=False):
        # poor man's async framework
        notifiy_queue.put(fatal_error)
    state, watcher = watch_cluster_group(zk, config['zgres_databases_path'], notify_main_thread)
    old_databases = None
    while True:
        fatal_error = notifiy_queue.get() # blocks till we get some new data from zookeeper
        if fatal_error:
            raise Exception('Got a fatal error, shutting down. Hopefully some info in the logs above! and hope systemd restarts us!')
        databases = state_to_databases(state)
        if old_databases != databases:
            with open('/var/lib/zgres/databases.json.tmp', 'w') as f:
                f.write(json.dumps(databases, sort_keys=True))
            os.rename('/var/lib/zgres/databases.json.tmp', '/var/lib/zgres/databases.json')
            check_call('zgres-apply') # apply the configuration to the machine

#
# Command Line Scripts
#

def _parse_args(parser, argv):
    # TODO: add args for setting loglevel here
    args = parser.parse_args(args=argv[1:])
    apply._setup_logging()
    return args

def sync_cli(argv=sys.argv):
    parser = argparse.ArgumentParser(description="""Start synchronization daemon
This daemon connects to zookeeper an watches for changes to the database config.
It then writes the config out to /var/lib/zgres/databases.json whenever there is a change
and calls zgres-apply.

This daemon gets run on all machines which need to know the database connection
info, that means appservers and probably database nodes if you use streaming
replication.
""")
    args = _parse_args(parser, argv)
    config = apply.Config()
    sys.exit(_sync(config))
