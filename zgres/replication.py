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
    def notify_conn_info(self, conn_info):
        self._current_conn_info = conn_info
        self._check_following()

    def _check_following(self):
        if self._current_master == self._am_following:
            return
        if self._current_master == self.app.my_id:
            return # I am the master, don't follow anyone!
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
    wal_replay_position = utils.pg_lsn_to_int(wal_replay_position)
    return wal_replay_position

class SelectFurthestAheadReplica:

    def __init__(self, name, app):
        self.name = name
        self.app = app

    @subscribe
    def best_replicas(self, states):
        nodes = [(wal_sort_key(state), id, state) for id, state in states]
        nodes.sort(reverse=True)
        best_key = None
        for sort_key, id, state in nodes:
            if best_key is None:
                # first key is the best hey
                best_key = sort_key
            if sort_key != best_key:
                break
            yield id, state

    @subscribe
    def veto_takeover(self, state):
        if state.get('pg_last_xlog_replay_location', None) is None:
            # we also need to know the replay location
            return True
        return False

    @subscribe
    def start_monitoring(self):
        loop = asyncio.get_event_loop()
        loop.call_soon(loop.create_task, self._set_replication_status())

    def _get_location(self, conn_args):
        conn = psycopg2.connect(**conn_args)
        try:
            cur = conn.cursor()
            cur.execute("SELECT pg_last_xlog_replay_location();")
            results = cur.fetchall()[0]
        finally:
            conn.rollback()
            conn.close()
        return results[0]

    async def _set_replication_status(self):
        args = None
        while True:
            await asyncio.sleep(1)
            try:
                if args is None:
                    # cache connection info till connection fails once
                    args = self.app.pg_connect_info()
                result = self._get_location(args)
            except psycopg2.OperationalError as e:
                logging.warn('Could not get wal location from postgresql: {}'.format(e))
                result = None
                args = None
            self.app.update_state(
                    pg_last_xlog_replay_location=result)
