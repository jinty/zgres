import sys
import time
import uuid
from copy import deepcopy
import asyncio
import logging
import argparse

import zgres._plugin
import zgres.config
from zgres import utils

_missing = object()

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

        dict(name='dcs_set_timeline',
            required=True,
            type='multiple'),
        dict(name='dcs_get_timeline',
            required=True,
            type='single'),

        dict(name='dcs_lock',
            required=True,
            type='single'),
        dict(name='dcs_unlock',
            required=True,
            type='multiple'),

        dict(name='dcs_delete_state',
            required=True,
            type='multiple'),
        dict(name='dcs_set_state',
            required=True,
            type='multiple'),
        dict(name='dcs_get_all_state',
            required=True,
            type='single'),

        dict(name='dcs_delete_conn',
            required=True,
            type='multiple'),
        dict(name='dcs_set_conn',
            required=True,
            type='multiple'),
        dict(name='dcs_get_all_conn',
            required=True,
            type='single'),

        dict(name='dcs_disconnect',
            required=True,
            type='multiple'),

        ######### Dealing with the local postgresql cluster
        dict(name='pg_connect_info', # return a dict with the connection info
            required=True,
            type='single'),
        dict(name='pg_get_database_identifier',
            required=True,
            type='single'),
        dict(name='pg_get_timeline',
            required=True,
            type='single'),
        # stop postgresql if it is not already stopped
        dict(name='pg_stop',
            required=True,
            type='multiple'),
        # start postgresql if it is not already running
        dict(name='pg_start',
            required=True,
            type='multiple'),
        # halt: should prevent the existing database from running again.
        # either stop the whole machine, move data directory aside, pg_rewind or prepare for re-bootstrapping as a slave
        dict(name='pg_reset',
            required=True,
            type='multiple'),
        # create a new postgresql database
        dict(name='pg_initdb',
            required=True,
            type='multiple'),
        dict(name='pg_stop_replication', # implement
            required=True,
            type='multiple'),

        # create a backup and put it where replicas can get it
        dict(name='pg_backup',
            required=True,
            type='multiple'),
        dict(name='pg_restore', # XXX -setup replication
            required=True,
            type='multiple'),
        dict(name='pg_am_i_replica',
            required=True,
            type='single'),

        # monitoring
        dict(name='start_monitoring',
            required=True,
            type='multiple'),
        ]

def wal_sort_key(state):
    if state['pg_is_in_recovery']:
        master_before_replica = 1
    else:
        master_before_replica = 0
    wal_replay_position = state.get('pg_last_xlog_replay_location', '0/0')
    wal_replay_position = -utils.pg_lsn_to_int(wal_replay_position)
    wal_recieve_position = state.get('pg_last_xlog_receive_location', '0/0')
    wal_recieve_position = -utils.pg_lsn_to_int(wal_recieve_position)
    return (master_before_replica, -wal_recieve_position, -wal_replay_position)

class App:

    _giveup_lock = asyncio.Lock()
    my_id = None
    config = None
    database_identifier = None

    def __init__(self, config):
        self.health_problems = {}
        self._conn_info = {}
        self._state = {}
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
        self._plugins.pg_stop()
        self._plugins.pg_restore()
        if not self._plugins.pg_am_i_replica():
            # destroy our current cluster
            self._plugins.pg_reset()
            logging.error("Something is seriously wrong: after restoring postgresql was NOT setup as a replica.")
            return 5
        return 0

    def master_bootstrap(self):
        # Bootstrap the master, make sure that the master can be
        # backed up and started before we set the database id
        self.logger.info('Initializing master DB')
        self._plugins.pg_initdb()
        self._plugins.pg_start()
        database_id = self._plugins.pg_get_database_identifier()
        self.logger.info('Initializing done, master database identifier: {}'.format(database_id))
        if self._plugins.dcs_lock('database_identifier'):
            self.logger.info('Got database identifer lock')
            if self._plugins.dcs_get_database_identifier() is not None:
                self.logger.info('Database identifier already set, restarting to become replica')
                return 0
            self.logger.info('No database identifer yet, performing first backup')
            self.database_identifier = database_id
            self._plugins.pg_backup()
            if not self._plugins.dcs_set_database_identifier(database_id):
                raise AssertionError('Something is VERY badly wrong.... this should never happen....')
            self.logger.info('Successfully bootstrapped master and set database identifier: {}'.format(database_id))
            return 0
        self.logger.info('Could not set database identifier in DCS. maybe another master beat us? trying again')
        return 5

    def initialize(self):
        """Initialize the application

        returns None if initialzation was successful
        or a number of seconds to wait before trying again to initialize
        """
        self._loop = asyncio.get_event_loop()
        self.unhealthy('zgres.initialize', 'Initializing')
        self.logger.info('Initializing plugins')
        self._plugins.initialize()
        if self._plugins.get_my_id:
            self.my_id = self._plugins.get_my_id()
        else:
            self.my_id = str(uuid.uuid1())
        self.logger.info('My ID is: {}'.format(self.my_id))
        their_database_id = self._plugins.dcs_get_database_identifier()
        if their_database_id is None:
            self.logger.info('Could not find database identifier in DCS, bootstrapping master')
            return self.master_bootstrap()
        self.logger.info('Found database identifier: {}'.format(their_database_id))
        my_database_id = self._plugins.pg_get_database_identifier()
        if my_database_id != their_database_id:
            self.logger.info('My database identifer is different ({}), bootstrapping as replica'.format(my_database_id))
            return self.replica_bootstrap()
        self.database_identifier = my_database_id
        am_replica = self._plugins.pg_am_i_replica()
        if not am_replica:
            if not self._plugins.dcs_lock('master'):
                self._plugins.pg_stop()
                my_timeline = self._plugins.pg_get_timeline()
                existing_timeline = self._plugins.dcs_get_timeline()
                if existing_timeline > my_timeline:
                    # a master has started while we didn't have the lock.
                    # we can't start again for risk of split brain
                    self._plugins.pg_reset()
                else:
                    self.logger.info('I could not get the master lock, but the master has not started up yet. (new master not functioning?) will try again in a bit')
                return 5
        self.logger.info('Making sure postgresql is running')
        self._plugins.pg_start()
        self.logger.info('Starting monitors')
        self._plugins.start_monitoring()
        self.healthy('zgres.initialize')
        if self.health_problems:
            if not am_replica:
                # I am an unhealthy master with the lock,
                # This is a wierd situation becase another master should have taken over before
                # we restarted and got the lock. let's check in a little while if we become healthy,
                # else try failover again
                self._loop.call_later(600, self._loop.create_task, self._handle_unhealthy_master())
        return None

    def update_state(self, _force=False, **kw):
        changed = _force
        for k, v in kw.items():
            v = deepcopy(v) # for reliable change detection on mutable args
            existing = self._state.get(k, _missing)
            if v != existing:
                changed = True
                self._state[k] = v
        if changed and 'zgres.initialize' not in self.health_problems:
            # don't update state in the DCS till we are finished updating
            self._plugins.dcs_set_state(self._state)

    def _update_timeline(self):
        my_timeline = self._plugins.pg_get_timeline()
        self._plugins.dcs_set_timeline(my_timeline)

    def master_lock_changed(self, owner):
        """Respond to a change in the maser lock"""
        self._master_lock_owner = owner
        if owner == self.my_id:
            # I should be the master
            if self._plugins.pg_am_i_replica():
                self._plugins.pg_stop_replication()
                self._update_timeline()
        else:
            if not self._plugins.pg_am_i_replica():
                # if I am master, wither the lock was deleted or someone else got it, shut down
                self.restart(10)
            if owner is None:
                self._loop.call_soon(self._loop.create_task, self._try_takeover())

    def am_i_best_replica(self):
        nodes = [(wal_sort_key(state), id, state) for id, state in self._plugins.dcs_get_all_state()]
        nodes.sort()
        best_key = None
        the_best = []
        for sort_key, id, state in nodes:
            if best_key is None:
                # first key is the best hey
                best_key = sort_key
            if sort_key != best_key:
                continue
            the_best.append(id)
            if id != self.my_id:
                continue
            if state['health_problems']:
                self.logger.info('Disqualifying myself because of health problems')
                continue
            if not state['pg_is_in_recovery']:
                self.logger.info('I am alreay a master.')
                continue
            # perform final check to see if I am willing
            if state['pg_last_xlog_replay_location'] != state['pg_last_xlog_receive_location']:
                self.logger.info('I have recieved the most logs, but not replayed them all yet, waiting a bit')
                return False
            return True
        self.logger.info('Not the best replica because these nodes were better than me: {}'.format(the_best))
        return False

    async def _try_takeover(self):
        while True:
            self.logger.info('Sleeping a little to allow state to be updated in the DCS before trying to take over')
            await asyncio.sleep(3) # let replicas update their state
            # The master is still missing and we should decide if we must take over
            if self._master_lock_owner is not None:
                self.logger.info('There is a new master: {}, stop trying to take over'.format(self._master_lock_owner))
                break
            if self.am_i_best_replica():
                # try get the master lock, if this suceeds, master_lock_change will be called again
                # and will bring us out of replication
                self.logger.info('I am one of the best, trying to get the master lock')
                self._plugins.dcs_lock('master')
            else:
                self.logger.info('I am not yet the best replica, giving the others a chance')

    def unhealthy(self, key, reason, can_be_replica=False):
        """Plugins call this if they want to declare the instance unhealthy.

        If an instance is unhealthy, but can continue to serve as a replica, set can_be_replica=True
        """
        self.health_problems[key] = dict(reason=reason, can_be_replica=can_be_replica)
        self.update_state(health_problems=self.health_problems)
        if 'zgres.initialize' in self.health_problems:
            return
        logging.warn('I am unhelthy: ({}) {}'.format(key, reason))
        if self._plugins.pg_am_i_replica():
            if not can_be_replica:
                self.dcs_remove_conn_info()
        else:
            self.dcs_remove_conn_info()
            self._loop.call_soon(self._loop.create_task, self._handle_unhealthy_master())

    async def _handle_unhealthy_master(self):
        if self._giveup_lock.locked():
            return # already trying
        async with self._giveup_lock:
            while self.health_problems:
                if self._plugins.is_there_willing_replica():
                    # fallover
                    self.restart(120)
                await loop.sleep(30)

    def healthy(self, key):
        """Plugins call this if they want to declare the instance unhealthy"""
        reason = self.health_problems.pop(key, _missing)
        if reason is _missing:
            return # no-op, we were already healthy
        self.update_state(health_problems=self.health_problems)
        logging.warn('Stopped being unhealthy for this reason: ({}) {}'.format(key, reason))
        if self.health_problems:
            logging.warn('I am still unhelthy for these reasons: {}'.format(self.health_problems))
        else:
            # YAY, we're healthy again
            if not self._plugins.pg_am_i_replica():
                locked = self._plugins.dcs_lock('master')
                if not locked:
                    # for some reason we cannot lock the master, restart and try again
                    self.restart(60) # give the
            self._set_conn_info()

    def _set_conn_info(self):
        self._plugins.dcs_set_conn(self._conn_info)

    @classmethod
    def run(cls, config):
        logging.info('Starting')
        app = App(config)
        timeout = app.initialize()
        if timeout is None:
            return
        app.restart(timeout)

    def restart(self, timeout):
        if not self._plugins.pg_am_i_replica():
            # If we are master, we must stop postgresql to avoid a split brain
            self._plugins.pg_stop()
        self._plugins.dcs_disconnect()
        logging.info('sleeping for {} seconds, then restarting'.format(timeout))
        time.sleep(timeout) # yes, this blocks everything. that's the point of it!
        sys.exit(0) # hopefully we get restarted immediately

    def pg_connect_info(self):
        # expose pg_connect for other plugins to use
        return self._plugins.pg_connect_info()

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

