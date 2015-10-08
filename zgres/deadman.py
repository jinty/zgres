import sys
import time
import uuid
import asyncio
import logging
import argparse

import zgres._plugin
import zgres.config
from zgres import utils

_PLUGIN_API = [
        # Run any initialization code plugins need. Allways called first. Return value ignored
        dict(name='initialize',
            required=False,
            type='multiple'),
        # Get the id of this postgresql cluster
        dict(name='get_my_id',
            required=False,
            type='single'),

        ######### Dealing with the Distributed Configuration system
        # set the database identifier, return True if it can be set, false if not.
        dict(name='dcs_set_database_identifier',
            required=True,
            type='multiple'),
        dict(name='dcs_get_database_identifier',
            required=True,
            type='single'),

        dict(name='dcs_lock',
            required=True,
            type='single'),
        dict(name='dcs_unlock',
            required=True,
            type='multiple'),
        dict(name='dcs_get_lock_owner',
            required=True,
            type='single'),

        dict(name='dcs_delete_info',
            required=True,
            type='multiple'),
        dict(name='dcs_set_info',
            required=True,
            type='multiple'),
        dict(name='dcs_get_info',
            required=True,
            type='multiple'),

        ######### Dealing with the local postgresql cluster
        dict(name='postgresql_connect', # get a superuser, local connection to postgresql
            required=True,
            type='single'),
        dict(name='postgresql_get_database_identifier',
            required=True,
            type='single'),
        # stop postgresql if it is not already stopped
        dict(name='postgresql_stop',
            required=True,
            type='multiple'),
        # start postgresql if it is not already running
        dict(name='postgresql_start',
            required=True,
            type='multiple'),
        # create a new postgresql database
        dict(name='postgresql_initdb',
            required=True,
            type='multiple'),

        # create a backup and put it where replicas can get it
        dict(name='postgresql_backup',
            required=True,
            type='multiple'),
        dict(name='postgresql_restore',
            required=True,
            type='multiple'),
        dict(name='postgresql_am_i_replica',
            required=True,
            type='single'),

        # monitoring
        dict(name='start_monitoring',
            required=True,
            type='multiple'),
        ]

class App:

    _giveup_lock = asyncio.Lock()

    def __init__(self, config):
        self.health_problems = {}
        self._conn_info = {}
        self._state_info = {}
        self.config = config
        self.tick_time = 1 # 1 second
        self._setup_plugins()
        self.logger = logging

    def _setup_plugins(self):
        self._plugins = zgres._plugin.get_plugins(
                self.config,
                'deadman',
                _PLUGIN_API,
                self)

    def replica_bootstrap(self):
        self._plugins.postgresql_stop()
        self._plugins.postgresql_restore()
        return 0

    def master_bootstrap(self):
        # Bootstrap the master, make sure that the master can be
        # backed up and started before we set the database id
        self._plugins.postgresql_initdb()
        self._plugins.postgresql_start()
        database_id = self._plugins.postgresql_get_database_identifier()
        if not self._plugins.dcs_lock('database_identifier'):
            return 60
        self._plugins.postgresql_backup()
        if self._plugins.dcs_set_database_identifier(database_id):
            self.logger.info('Successfully bootstrapped master and set database identifier: {}'.format(database_id))
        return 0

    def initialize(self):
        """Initialize the application

        returns None if initialzation was successful
        or a number of seconds to wait before trying again to initialize
        """
        self._loop = asyncio.get_event_loop()
        self.unhealthy('zgres.initialize', 'Initializing')
        self._plugins.initialize()
        if self._plugins.get_my_id:
            self.my_id = self._plugins.get_my_id()
        else:
            self.my_id = str(uuid.uuid1())
        their_database_id = self._plugins.dcs_get_database_identifier()
        if their_database_id is None:
            return self.master_bootstrap()
        my_database_id = self._plugins.postgresql_get_database_identifier()
        if my_database_id != their_database_id:
            return self.replica_bootstrap()
        am_replica = self._plugins.postgresql_am_i_replica()
        if not am_replica:
            if not self._plugins.dcs_lock('master'):
                self._plugins.postgresql_stop()
                if self.is_master_ahead():
                    # there is already another master and it has moved ahead of us
                    self._plugins.halt() # should irreperably stop postgresql from running again
                                         # either stop the whole machine, move data directory
                return 60
        self._plugins.postgresql_start()
        self._plugins.start_monitoring()
        self.healthy('zgres.initialize')
        if not am_replica and self.health_problems:
            # I am an unhealthy master with the lock,
            # This is a wierd situation becase another master should have taken over before
            # we restarted and got the lock. let's check in a little while if we become healthy,
            # else try failover again
            self._loop.call_later(600, self._loop.create_task, self._handle_unhealthy_master())
        return None

    def master_locked(self, by_me):
        self.master_lock = True
        if by_me:
            self._plugins.stop_replication()
        else:
            if not self._plugins.postgresql_am_i_replica():
                # a new master just appeared, it's not me
                # I'm also a master, er so boom...
                call.postgresql_stop()
                self.restart(0)

    def master_unlocked(self):
        """Respond to an event where the master is unlocked"""
        self.master_lock = False
        self._loop.call_soon(self._handle_master_unlocked)

    async def _handle_master_unlocked(self):
        if self.master_lock:
            return
        if not self._plugins.postgresql_am_i_replica():
            # we are not a replica, and the master lock was lost. Let's wait a bit for
            # another slave to takeover and then restart
            self._plugins.stop_postgresql()
            self.restart(120)
        while True:
            # The master is missing and we should decide if we must take over
            await loop.sleep(self.replication_update_interval * 3) # let replicas update their state
            if self.master_lock:
                return
            if not self.am_i_best_replica():
                await loop.sleep(self.replication_update_interval * 10)
                continue
            if self._plugins.dcs_lock_master():
                # the "master_locked" event should stop replication now
                return

    def unhealthy(self, key, reason):
        """Plugins call this if they want to declare the instance unhealthy"""
        self.health_problems[key] = reason
        if 'zgres.initialize' in self.health_problems:
            return
        logging.warn('I am unhelthy: ({}) {}'.format(key, reason))
        self.dcs_remove_conn_info()
        if not self._plugins.postgresql_am_i_replica():
            self._loop.call_soon(self._loop.create_task, self._handle_unhealthy_master())

    async def _handle_unhealthy_master(self):
        if self._giveup_lock.locked():
            return # already trying
        async with self._giveup_lock:
            while self.health_problems:
                if self._plugins.is_there_willing_replica():
                    # fallover
                    self._plugins.stop_postgresql()
                    self.restart(120)
                await loop.sleep(30)

    def healthy(self, key):
        """Plugins call this if they want to declare the instance unhealthy"""
        reason = self.health_problems.pop(key)
        logging.warn('Stopped being unhealthy for this reason: ({}) {}'.format(key, reason))
        if self.health_problems:
            logging.warn('I am still unhelthy for these reasons: {}'.format(self.health_problems))
        else:
            # YAY, we're healthy again
            if not self._plugins.postgresql_am_i_replica():
                locked = self._plugins.dcs_lock('master')
                if not locked:
                    # for some reason we cannot lock the master, restart and try again
                    self.restart(60) # give the
            self._plugins.dcs_set_info('conn', self._conn_info)

    @classmethod
    def run(cls, config):
        logging.info('Starting')
        app = App(config)
        timeout = app.initialize()
        if timeout is None:
            logging.info('Finished Initialization, starting to monitor. I am a {}'.format(self.health_problems))
            return
        app.restart(timeout)

    def restart(self, timeout):
        self._plugins.dcs_unlock('master')
        self._plugins.dcs_delete_info('state')
        self._plugins.dcs_delete_info('conn')
        logging.info('sleeping for {} seconds, then restarting'.format(timeout))
        time.sleep(timeout) # yes, this blocks everything. that's the point of it!
        sys.exit(0) # hopefully we get restarted immediately

    def postgresql_connect(self):
        # expose postgresql_connect for other plugins to use
        return self._plugins.postgresql_connect()

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

