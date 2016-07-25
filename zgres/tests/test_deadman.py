from unittest.mock import call, patch, Mock
import asyncio

import pytest

from . import FakeSleeper
from . import deadman_app
from . import mock_plugin


def fake_best_replicas(replicas):
    l = sorted(replicas, key=lambda x: x[1]['willing'])
    if l:
        winner = l[0][1]['willing']
        l = [i for i in l if winner == i[1]['willing']]
    for id, state in l:
        yield id, state

def mock_state(replica=False, **kw):
    if replica:
        defaults = dict(
                health_problems={},
                replica=replica,
                pg_last_xlog_replay_location='68A/16E1DA8',
                pg_last_xlog_receive_location='68A/16E1DA8')
    else:
        defaults = dict(
                health_problems={},
                replica=replica,
                pg_current_xlog_location='68A/16E1DA8')
    defaults.update(kw)
    return defaults

@pytest.fixture
def app(deadman_app):
    return deadman_app(dict(deadman=dict(tick_time=1)))

NO_SUBSCRIBER = object()

def state_getter(app, *extra_states):
    def dcs_list_state():
        # generate some mock state
        for id, state in [(app.my_id, app._state)] + list(extra_states):
            yield id, state
    return dcs_list_state

def setup_plugins(app, **kw):
    plugin = mock_plugin(app._pm)
    plugin.best_replicas.side_effect = fake_best_replicas
    get_my_id = kw.get('get_my_id', '42')
    pg_replication_role = kw.get('pg_replication_role', 'replica')
    defaults = {
            'pg_replication_role': pg_replication_role,
            'pg_get_timeline': 1,
            'dcs_get_timeline': 1,
            'get_conn_info': dict(host='127.0.0.1'),
            'get_my_id': get_my_id,
            'dcs_set_database_identifier': True,
            'dcs_get_database_identifier': '12345',
            'pg_get_database_identifier': '12345',
            }
    if pg_replication_role == 'master':
        defaults['dcs_lock'] = True
    defaults.update(kw)
    for k, v in defaults.items():
        func = getattr(plugin, k)
        func.return_value = v
    return plugin

def test_master_bootstrap(app):
    plugins = setup_plugins(app,
            dcs_get_database_identifier=None,
            dcs_lock=True,
            pg_get_database_identifier='42')
    timeout = app.initialize()
    assert plugins.mock_calls ==  [
            call.initialize(),
            call.get_my_id(),
            # check if we have a db identifier set
            call.dcs_get_database_identifier(),
            # no, ok, init our db
            call.pg_initdb(),
            # make sure it starts
            call.pg_start(),
            call.pg_get_database_identifier(),
            # lock the database identifier so no-one else gets here
            call.dcs_lock('database_identifier'),
            # while locked make sure there is no id set in the DCS before we got the lock
            call.dcs_get_database_identifier(),
            # Make the first backup while locked with no DCS
            call.pg_backup(),
            # set the database identifier AFTER
            call.dcs_set_database_identifier('42')
            ]
    # shut down cleanly and immediately
    assert timeout == 0

def test_master_boostrap_fails_to_lock_db_id(app):
    plugins = setup_plugins(app,
            dcs_get_database_identifier=None,
            dcs_lock=False,
            pg_get_database_identifier='42')
    timeout = app.initialize()
    assert plugins.mock_calls ==  [
            call.initialize(),
            call.get_my_id(),
            # check if we have a db identifier set
            call.dcs_get_database_identifier(),
            # no, ok, init our db
            call.pg_initdb(),
            # make sure it starts
            call.pg_start(),
            call.pg_get_database_identifier(),
            # lock the database identifier so no-one else gets here
            call.dcs_lock('database_identifier')
            ]
    # shut down cleanly
    assert timeout == 5

def test_replica_bootstrap(app):
    plugins = setup_plugins(app,
            dcs_get_database_identifier='1234')
    plugins.pg_get_database_identifier.side_effect = ['42', '1234']
    timeout = app.initialize()
    assert plugins.mock_calls ==  [
            call.initialize(),
            call.get_my_id(),
            # compare our id with the id in the DCS
            call.dcs_get_database_identifier(),
            call.pg_get_database_identifier(),
            # make sure postgresql is stopped
            call.pg_stop(),
            # postgresql restore
            call.pg_initdb(),
            call.pg_restore(),
            call.pg_setup_replication(None),
            call.pg_get_database_identifier(),
            call.pg_replication_role()
            ]
    # shut down cleanly and immediately
    assert timeout == 0

def test_replica_bootstrap_fails_sanity_test(app):
    plugins = setup_plugins(app,
            pg_replication_role='master',
            dcs_get_database_identifier='1234',
            pg_get_database_identifier='42')
    timeout = app.initialize()
    assert plugins.mock_calls ==  [
            call.initialize(),
            call.get_my_id(),
            # compare our id with the id in the DCS
            call.dcs_get_database_identifier(),
            call.pg_get_database_identifier(),
            # make sure postgresql is stopped
            call.pg_stop(),
            # postgresql restore
            call.pg_initdb(),
            call.pg_restore(),
            call.pg_setup_replication(None),
            call.pg_get_database_identifier(),
            call.pg_replication_role(),
            call.pg_reset(),
            ]
    # shut down after 5 seconds to try again
    assert timeout == 5

@pytest.mark.asyncio
async def test_master_start(app):
    plugins = setup_plugins(app,
            dcs_get_database_identifier='1234',
            dcs_lock=True,
            pg_replication_role='master',
            pg_get_database_identifier='1234')
    def start_monitoring():
        app.unhealthy('test_monitor', 'Waiting for first check')
    plugins.start_monitoring.side_effect = start_monitoring
    # sync startup
    timeout = app.initialize()
    assert plugins.mock_calls ==  [
            call.initialize(),
            call.get_my_id(),
            # compare our id with the id in the DCS
            call.dcs_get_database_identifier(),
            call.pg_get_database_identifier(),
            # check if I am a replica
            call.pg_replication_role(),
            # no, so check if there is a master
            call.dcs_lock('master'),
            # no master, so sure the DB is running
            call.pg_start(),
            # start monitoring
            call.start_monitoring(),
            call.dcs_watch(
                app.master_lock_changed,
                app._notify_state,
                app._notify_conn_info,
            ),
            call.get_conn_info(),
            # set our first state
            call.dcs_set_state({
                'host': '127.0.0.1',
                'replication_role': 'master',
                'health_problems': {'test_monitor':
                    {'can_be_replica': False, 'reason': 'Waiting for first check'}}})
            ]
    # Carry on running afterwards
    assert timeout == None
    assert app.health_problems == {'test_monitor': {'can_be_replica': False, 'reason': 'Waiting for first check'}}
    # Our test monitor becomes healthy
    plugins.reset_mock()
    app.healthy('test_monitor')
    assert plugins.mock_calls ==  [
            call.dcs_set_state({
                'host': '127.0.0.1',
                'replication_role': 'master',
                'health_problems': {}}),
            call.pg_replication_role(),
            call.dcs_lock('master'),
            call.dcs_set_conn_info({'host': '127.0.0.1'}),
           ]

def test_failed_over_master_start(app):
    # A master has failed over and restarted, another master has sucessfully advanced
    plugins = setup_plugins(app,
            dcs_lock=False,
            dcs_get_timeline=2,
            pg_get_timeline=1,
            pg_replication_role='master')
    # sync startup
    timeout = app.initialize()
    assert plugins.mock_calls ==  [
            call.initialize(),
            call.get_my_id(),
            # compare our id with the id in the DCS
            call.dcs_get_database_identifier(),
            call.pg_get_database_identifier(),
            # check if I am a replica
            call.pg_replication_role(),
            # no, so check if there is a master
            call.dcs_lock('master'),
            call.dcs_get_lock_owner('master'),
            call.pg_stop(),
            # compare our timeline to what's in the DCS
            call.pg_get_timeline(),
            call.dcs_get_timeline(),
            # we're on an older timeline, so reset
            call.pg_reset(),
            ]
    # Carry on running afterwards
    assert timeout == 5

def test_replica_start(app):
    plugins = setup_plugins(app,
            dcs_get_database_identifier='1234',
            dcs_lock=True,
            pg_replication_role='replica',
            pg_get_database_identifier='1234')
    app._conn_info['a'] = 'b'
    def start_monitoring():
        app.unhealthy('test_monitor', 'Waiting for first check')
    plugins.start_monitoring.side_effect = start_monitoring
    # sync startup
    timeout = app.initialize()
    assert plugins.mock_calls ==  [
            call.initialize(),
            call.get_my_id(),
            # compare our id with the id in the DCS
            call.dcs_get_database_identifier(),
            call.pg_get_database_identifier(),
            # check if I am a replica
            call.pg_replication_role(),
            # not master, so sure the DB is running
            call.pg_start(),
            # start monitoring
            call.start_monitoring(),
            call.dcs_watch(
                app.master_lock_changed,
                app._notify_state,
                app._notify_conn_info,
            ),
            # setup our connection info
            call.get_conn_info(),
            # set our first state
            call.dcs_set_state({
                'a': 'b',
                'host': '127.0.0.1',
                'replication_role': 'replica',
                'health_problems': {'test_monitor':
                    {'can_be_replica': False, 'reason': 'Waiting for first check'}},
                })
            ]
    # Carry on running afterwards
    assert timeout == None
    assert app.health_problems == {'test_monitor': {'can_be_replica': False, 'reason': 'Waiting for first check'}}
    # Our test monitor becomes healthy
    plugins.reset_mock()
    with patch('time.time') as mock_time:
        app.healthy('test_monitor')
    assert plugins.mock_calls ==  [
            call.veto_takeover({'health_problems': {},
                'a': 'b',
                'replication_role': 'replica',
                'host': '127.0.0.1'}),
            call.dcs_set_state({'health_problems': {},
                'a': 'b',
                'replication_role': 'replica',
                'host': '127.0.0.1',
                'willing': mock_time(),
                }),
            call.pg_replication_role(),
            call.dcs_set_conn_info({'a': 'b', 'host': '127.0.0.1'}),
           ]

def test_plugin_subscribe_to_state(app):
    plugins = setup_plugins(app)
    app.initialize()
    assert plugins.dcs_watch.mock_calls ==  [
            call.dcs_watch(
                app.master_lock_changed,
                app._notify_state,
                app._notify_conn_info,
            )]

def test_plugin_tells_app_to_follow_new_leader(app):
    plugins = setup_plugins(app)
    app.initialize()
    plugins.reset_mock()
    app.follow(primary_conninfo=dict(host='127.0.0.9', port=5432))
    assert plugins.mock_calls == [
            call.pg_replication_role(),
            call.pg_setup_replication({'port': 5432, 'host': '127.0.0.9'}),
            call.pg_restart()] # must restart for new recovery.conf to take effect

def test_restart_master(app, event_loop):
    plugins = setup_plugins(app,
            pg_replication_role='master')
    app.initialize()
    plugins.reset_mock()
    with patch('time.sleep') as sleep:
        app.restart(10)
        assert sleep.called_once_with(10)
    event_loop.run_forever() # must be stopped by restart()
    assert plugins.mock_calls ==  [
            call.pg_replication_role(),
            call.pg_stop(),
            call.dcs_disconnect()
            ]

def test_restart_replica(app, event_loop):
    plugins = setup_plugins(app,
            pg_replication_role='replica')
    app.initialize()
    plugins.reset_mock()
    with patch('time.sleep') as sleep:
        app.restart(10)
        assert sleep.called_once_with(10)
    event_loop.run_forever() # must be stopped by restart()
    assert plugins.mock_calls ==  [
            call.pg_replication_role(),
            call.dcs_disconnect()
            ]

@pytest.mark.asyncio
async def test_master_lock_broken(app):
    plugins = setup_plugins(app,
            pg_replication_role='master')
    assert app.initialize() == None
    plugins.reset_mock()
    # if the lock is broken, shutdown postgresql and exist
    with patch('time.sleep') as sleep:
        with patch('sys.exit') as exit:
            app.master_lock_changed(None)
            assert exit.called_once_with(0)
        assert sleep.called_once_with(10)
    assert plugins.mock_calls ==  [
            call.pg_replication_role(),
            call.pg_replication_role(),
            call.pg_stop(),
            call.dcs_disconnect(),
            call.master_lock_changed(None)
            ]
    assert app._master_lock_owner == None

@pytest.mark.asyncio
async def test_master_lock_changes_owner(app):
    # if the lock changes owner to someone else, shutdown postgresql and exist
    plugins = setup_plugins(app,
            pg_replication_role='master')
    assert app.initialize() == None
    plugins.reset_mock()
    with patch('time.sleep') as sleep:
        with patch('sys.exit') as exit:
            app.master_lock_changed('someone else')
            assert exit.called_once_with(0)
        assert sleep.called_once_with(10)
    assert plugins.mock_calls ==  [
            call.pg_replication_role(),
            call.pg_replication_role(),
            call.pg_stop(),
            call.dcs_disconnect(),
            call.master_lock_changed('someone else')
            ]
    assert app._master_lock_owner == 'someone else'
    # if the lock is owned by us, carry on trucking
    plugins.reset_mock()
    with patch('time.sleep') as sleep:
        with patch('sys.exit') as exit:
            app.master_lock_changed(app.my_id)
            assert exit.called_once_with(0)
        assert sleep.called_once_with(10)
    assert plugins.mock_calls ==  [
            call.pg_replication_role(),
            call.master_lock_changed('42')
            ]
    assert app._master_lock_owner == app.my_id

@pytest.mark.asyncio
async def test_plugin_subscribes_to_master_lock_change(app):
    plugins = setup_plugins(app,
            pg_get_timeline=42,
            master_lock_changed=[('pluginA', None)],
            pg_replication_role='replica')
    assert app.initialize() == None
    plugins.reset_mock()
    app.master_lock_changed('someone else')
    assert plugins.mock_calls ==  [
            call.pg_replication_role(),
            call.master_lock_changed('someone else'),
            ]

@pytest.mark.asyncio
async def test_replica_reaction_to_master_lock_change(app):
    plugins = setup_plugins(app,
            pg_get_timeline=42,
            pg_replication_role='replica')
    assert app.initialize() == None
    plugins.reset_mock()
    # if the lock changes owner to someone else, carry on trucking
    plugins.reset_mock()
    app.master_lock_changed('someone else')
    assert plugins.mock_calls ==  [
            call.pg_replication_role(),
            call.master_lock_changed('someone else')
            ]
    assert app._master_lock_owner == 'someone else'
    # if the lock is owned by us, er, we stop replication and become the master
    plugins.reset_mock()
    plugins.pg_replication_role.side_effect = ['replica', 'master']
    app.master_lock_changed(app.my_id)
    assert plugins.mock_calls ==  [
            call.pg_replication_role(),
            call.dcs_set_state({
                'replication_role': 'taking-over',
                'willing': None,
                'health_problems': {},
                'host': '127.0.0.1'}),
            call.pg_stop_replication(),
            call.pg_replication_role(),
            call.pg_get_timeline(),
            call.dcs_set_timeline(42),
            call.dcs_set_state({
                'health_problems': {},
                'replication_role': 'master',
                'willing': None,
                'host': '127.0.0.1'}),
            call.master_lock_changed('42')
            ]
    assert app._master_lock_owner == app.my_id

@pytest.mark.asyncio
async def test_replica_tries_to_take_over(app):
    plugins = setup_plugins(app,
            pg_replication_role='replica')
    assert app.initialize() == None
    plugins.reset_mock()
    # if there is no lock owner, we start looping trying to become master
    app.master_lock_changed(None)
    assert plugins.mock_calls ==  [call.pg_replication_role(), call.master_lock_changed(None)]
    plugins.reset_mock()
    from asyncio import sleep as real_sleep
    with patch('asyncio.sleep') as sleep:
        sleeper = FakeSleeper()
        sleep.side_effect = sleeper
        # the first thing is to sleep a bit
        await sleeper.next()
        assert sleeper.log == [3]
        assert plugins.mock_calls == []
        # takeover attempted
        states = [(app.my_id, {'willing': 99.0}), (app.my_id, {'willing': 100.0})]
        plugins.dcs_list_state.return_value = states
        await sleeper.next()
        assert sleeper.log == [3, 3]
        assert plugins.mock_calls ==  [
                call.dcs_list_state(),
                call.best_replicas([('42', {'willing': 99.0}), ('42', {'willing': 100.0})]),
                call.dcs_lock('master')]

def test_replica_unhealthy(app):
    plugins = setup_plugins(app,
            pg_replication_role='replica')
    app.initialize()
    plugins.reset_mock()
    app.unhealthy('boom', 'It went Boom')
    assert plugins.mock_calls ==  [
            call.dcs_set_state({
                'host': '127.0.0.1',
                'replication_role': 'replica',
                'willing': None, # I am not going to participate in master elections
                'health_problems': {'boom': {'reason': 'It went Boom', 'can_be_replica': False}}}),
            call.pg_replication_role(),
            call.dcs_delete_conn_info(),
            ]

def test_replica_slightly_sick(app):
    plugins = setup_plugins(app,
            pg_replication_role='replica')
    app.initialize()
    plugins.reset_mock()
    app.unhealthy('boom', 'It went Boom', can_be_replica=True)
    assert plugins.mock_calls ==  [
            call.dcs_set_state({
                'host': '127.0.0.1',
                'replication_role': 'replica',
                'willing': None, # I am not going to participate in master elections
                'health_problems': {'boom': {'reason': 'It went Boom', 'can_be_replica': True}}}),
            call.pg_replication_role(),
            ]

@pytest.mark.asyncio
async def test_master_unhealthy(app):
    plugins = setup_plugins(app,
            pg_replication_role='master')
    app.initialize()
    plugins.reset_mock()
    app.unhealthy('boom', 'It went Boom', can_be_replica=True)
    assert plugins.mock_calls ==  [
            call.dcs_set_state({
                'host': '127.0.0.1',
                'replication_role': 'master',
                'health_problems': {'boom': {'reason': 'It went Boom', 'can_be_replica': True}}}),
            call.pg_replication_role(),
            call.dcs_delete_conn_info(),
            ]
    plugins.reset_mock()
    # now we should have _handle_unhealthy_master running
    with patch('asyncio.sleep') as sleep, patch('zgres.deadman.App._stop') as exit, patch('time.sleep') as blocking_sleep:
        sleeper = FakeSleeper()
        sleep.side_effect = sleeper
        exit.side_effect = lambda : sleeper.finish()
        # there is no replica, so we just sleep and ping the
        # DCS to find a willing replica
        states = [iter([])]
        plugins.dcs_list_state.side_effect = states
        await sleeper.next()
        assert plugins.mock_calls == [call.dcs_list_state()]
        # we add a willing replica
        states = [iter([('other', {'willing': 1})])]
        plugins.dcs_list_state.side_effect = states
        plugins.reset_mock()
        await sleeper.next()
        assert plugins.mock_calls == [
                call.dcs_list_state(),
                call.pg_replication_role(),
                call.pg_stop(),
                call.dcs_disconnect()
                ]
