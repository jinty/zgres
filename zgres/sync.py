import os
import sys
import argparse

import zgres.plugin
import zgres.config
from zgres import utils

class SyncApp:
    """Synchronize local machine configuration with the current postgresql state.

    Call our handler plugins with new state as it becomes available from the source plugin.

    This class defines 3 events which plugins can define:
        start_watching: One and only one plugin must define this event, it is called with no arguments
                        and should start watching the cluster configuration. This plugin is responsible
                        for calling SyncApp.conn_info and SyncApp.state as needed when the cluster
                        configuration changes.
                        see zgres.zookeeper.ZooKeeperSource for an example of this plugin
        conn_info: Called whenever the connection information of the cluster changes (e.g. a new replica or master).
                   see zgres.apply.Plugin for an example of this plugin.
        state: Called whenever the additional data of a cluster node changes (e.g. the replication lag). 

    All plugins are configured by being passed the arguments: (plugin name, SyncApp())
    """

    def __init__(self, config):
        self.config = config
        self._plugins = zgres.plugin.get_plugins(config, 'sync', ['state', 'conn_info', dict(name='start_watching', required=True, type='single')], self)
        if self._plugins.state is None and self._plugins.conn_info is None:
            raise Exception('No plugins configured for zgres-sync')
        if self._plugins.state is None:
            self.state = None
        if self._plugins.conn_info is None:
            self.conn_info = None
        self._plugins.start_watching() # start watching for cluster events.

    def conn_info(self, databases):
        self._plugins.conn_info(databases)

    def state(self, databases):
        self._plugins.state(databases)


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
