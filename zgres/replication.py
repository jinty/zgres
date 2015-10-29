import logging
import asyncio

import psycopg2

from .plugin import subscribe
from . import utils

class FollowTheLeader:

    _am_following = None
    _current_master = None
    _current_conn_info = None

    def __init__(self, name, app):
        self.name = name
        self.app = app

    @subscribe
    def master_lock_changed(self, owner):
        self._current_master = owner
        self._check_following()

    @subscribe
    def notify_conn_info(self, connection_info):
        self._current_conn_info = connection_info
        self._check_following()

    def _check_following(self):
        if self._current_master == self._am_following:
            return
        if self._current_conn_info is not None and self._current_master in self._current_conn_info:
            master_info = self._current_conn_info.get(self._current_master)
            self.app.follow(dict(
                host=master_info['host'],
                port=master_info.get('port', '5432')))
            self._am_following = self._current_master

def wal_sort_key(state):
    wal_replay_position = state.get('pg_last_xlog_replay_location', None)
    if wal_replay_position is None:
        wal_replay_position = '0/0'
    wal_replay_position = -utils.pg_lsn_to_int(wal_replay_position)
    wal_recieve_position = state.get('pg_last_xlog_receive_location', None)
    if wal_recieve_position is None:
        wal_recieve_position = '0/0'
    wal_recieve_position = -utils.pg_lsn_to_int(wal_recieve_position)
    return (-wal_recieve_position, -wal_replay_position)

class SelectFurthestAheadReplica:

    def __init__(self, name, app):
        self.name = name
        self.app = app

    @subscribe
    def best_replicas(self, states):
        nodes = [(wal_sort_key(state), id, state) for id, state in states]
        nodes.sort()
        best_key = None
        for sort_key, id, state in nodes:
            if best_key is None:
                # first key is the best hey
                best_key = sort_key
            if sort_key != best_key:
                break
            yield id, state

    @subscribe
    def willing_replicas(self, states):
        for id, state in states:
            if state.get('nofailover', False):
                continue
            if state.get('health_problems', True):
                # if missing, something is wrong, should be an empty dict
                continue
            if not state.get('replica', False):
                continue
            if state.get('pg_last_xlog_receive_location', None) is None \
                    or state.get('pg_last_xlog_replay_location', None) is None:
                # we also need to know the replay location
                continue
            yield id, state

    @subscribe
    def start_monitoring(self):
        loop = asyncio.get_event_loop()
        loop.call_soon(loop.create_task, self._set_replication_status())

    def _get_location(self, conn_args):
        conn = psycopg2.connect(**conn_args)
        try:
            cur = conn.cursor()
            cur.execute("SELECT pg_last_xlog_replay_location(), pg_last_xlog_receive_location()")
            results = cur.fetchall()[0]
        finally:
            conn.rollback()
            conn.close()
        return results

    async def _set_replication_status(self):
        while True:
            await asyncio.sleep(1)
            args = self.app.pg_connect_info()
            try:
                results = _get_location(self, args)
            except psycopg2.OperationalError as e:
                logging.warn('Could not get wal location from postgresql: {}'.format(e))
                results = (None, None)
            self.app.update_state(
                    pg_last_xlog_replay_location=results[0],
                    pg_last_xlog_receive_location=results[1])
