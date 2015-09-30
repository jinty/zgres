import sys
import asyncio
import logging
import argparse

import zgres._plugin
import zgres.config
from zgres import utils

class App:

    state = {}

    def __init__(self, config):
        self.config = config
        self._plugins = zgres._plugin.get_plugins(config, 'deadman', [
            dict(name='postgresql_stop', required=True, type='multiple'),
            dict(name='is_database_identifier_set', required=True, type='multiple'),
            'initialize',
            ], self)

    def intialize(self):
        """Initialize the application

        returns None if initialzation was successful
        or a number of seconds to wait before trying again to initialize
        """
        self.unhealthy('zgres.initialize', 'Initializing')
        self._plugins.initialize()
        their_database_id = self._plugins.dcs_get_database_identifier():
        if their_database_id is None:
            self.master_bootstrap()
            return 0
        my_database_id = self._plugins.postgresql_get_database_identifier()
        if my_database_id != their_database_id:
            self.replica_bootstrap()
            return 0
        am_replica = self._plugins.postgresql_am_i_replica()
        if not am_replica:
            master_locked = self.is_master_locked()
            if maseter_locked:
                # there is already another master, so we die...
                self._plugins.postgresql_stop()
                self._plugins.halt() # should irreperably stop the whole machine
                return 60
        self._plugins.postgresql_start()
        self._plugins.start_monitoring()
        zgres.healthy('zgres.initialize')
        return None

    def slave_takeover(self):
        # Some plugin (probably the DCS plugin) calls this to try take over from the master
        locked = self._plugins.lock_master()
        if locked:
            self._plugins.postgresql_stop_replicating()

    def master_failover(self):
        if not state:
            return
        willing_replica = self._plugins.dcs_have_willing_replica()
        if not willing_replica:
            loop.call_later(60, self.master_failover())
        else:
            self._plugins.stop_postgresql()
            self.dcs_remove_state_info()
            self.dcs_unlock_master()
            self.restart(120) # give the

    def unhealthy(self, key, reason):
        """Plugins call this if they want to declare the instance unhealthy"""
        assert key not in self.health_state
        self.health_state[key] = reason
        if 'zgres.initialize' in self.health_state:
            return
        logging.warn('I am unhelthy: ({}) {}'.format(key, reason))
        self.dcs_remove_conn_info()
        if not self._plugins.postgresql_am_i_replica() or self._plugins.master_locked():
            self.master_failover()

    def healthy(self, key):
        """Plugins call this if they want to declare the instance unhealthy"""
        reason = self.health_state.pop(key)
        logging.warn('Stopped being unhealthy for this reason: ({}) {}'.format(key, reason))
        if self.health_state:
            logging.warn('I am still unhelthy for these reasons: {}'.format(self.health_state))
        else:
            # YAY, we're healthy again
            if not self._plugins.postgresql_am_i_replica():
                locked = self._plugins.lock_master()
                if not locked:
                    # for some reason we cannot lock the master, restart and try again
                    self.restart(60) # give the
            self.dcs_set_conn_info()

    @classmethod
    def run(cls, config):
        logging.info('Starting')
        app = App(Config)
        timeout = app.initialize()
        if timeout is None:
            logging.info('Finished Initialization, starting to monitor. I am a {}'.format(self.health_state))
            return
        app.restart(timeout)

    @classmethod
    def restart(self, timeout):
        logging.info('sleeping for {} seconds, then restarting'.format(timeout))
        time.sleep(timeout) # yes, this blocks everything. that's the point of it!
        sys.exit(0) # hopefully we get restarted immediately

#
# Command Line Scripts
#

def deadman_cli(argv=sys.argv):
    parser = argparse.ArgumentParser(description="""Monitors/controls the local postgresql installation.

This daemon will do these things:

    - Register the local postgresql instance with Zookeeper by creating a file
      named the IP address to connect on.
    - Try to become master by creating the file:
        master-{cluster_name}
      in zookeeper. If we suceed we create the file /tmp/zgres_become_master.
    - Shutdown postgres temporarily if we are master and the zookeeper connection is lost.
    - Shutdown postgres permanently if master-{cluster_name} already exists and we didn't create it
        (split-brain avoidance)
    - Monitor the local postgresql installation, if it becomes unavailable,
      withdraw our zookeeper registrations.

It does not:
    - maintain streaming replication (use zgres-apply hooks for that)
    - do remastering (assumed to have happened before we start)
""")
    config = zgres.config.parse_args(parser, argv)
    result = utils.run_asyncio(App.run, config)
    sys.exit(result)

