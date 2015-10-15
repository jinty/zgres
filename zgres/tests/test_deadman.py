from unittest.mock import call, patch, Mock
import asyncio

import pytest

from . import FakeSleeper

@pytest.fixture
def app():
    from ..deadman import App, _PLUGIN_API
    with patch('zgres._plugin.get_plugins') as get_plugins:
        get_plugins.return_value = Mock(spec_set=[s['name'] for s in _PLUGIN_API])
        app = App({})
    plugins = app._plugins
    return app, app._plugins

def setup_plugins(plugins, **kw):
    from ..deadman import _PLUGIN_API
    postgresql_am_i_replica = kw.get('postgresql_am_i_replica', True)
    defaults = {
            'postgresql_am_i_replica': postgresql_am_i_replica,
            'dcs_get_database_identifier': '12345',
            'postgresql_get_database_identifier': '12345',
            }
    if not postgresql_am_i_replica:
        defaults['dcs_lock'] = True
    defaults.update(kw)
    for k, v in defaults.items():
        for i in _PLUGIN_API:
            if k == i['name']:
                if i['type'] == 'multiple':
                    # assert that our test plugin data is really unpackable
                    z = [(i, k) for i, k in v]
                    v = iter(z) # make it a real iterable
                break
        getattr(plugins, k).return_value = v

def test_master_bootstrap(app):
    app, plugins = app
    setup_plugins(
            plugins,
            dcs_get_database_identifier=None,
            dcs_lock=True,
            postgresql_get_database_identifier='42')
    timeout = app.initialize()
    assert app._plugins.mock_calls ==  [
            call.initialize(),
            call.get_my_id(),
            # check if we have a db identifier set
            call.dcs_get_database_identifier(),
            # no, ok, init our db
            call.postgresql_initdb(),
            # make sure it starts
            call.postgresql_start(),
            call.postgresql_get_database_identifier(),
            # lock the database identifier so no-one else gets here
            call.dcs_lock('database_identifier'),
            # while locked make sure there is no id set in the DCS before we got the lock
            call.dcs_get_database_identifier(),
            # Make the first backup while locked with no DCS
            call.postgresql_backup(),
            # set the database identifier AFTER
            call.dcs_set_database_identifier('42')
            ]
    # shut down cleanly and immediately
    assert timeout == 0

def test_master_boostrap_fails_to_lock_db_id(app):
    app, plugins = app
    setup_plugins(
            plugins,
            dcs_get_database_identifier=None,
            dcs_lock=False,
            postgresql_get_database_identifier='42')
    timeout = app.initialize()
    assert app._plugins.mock_calls ==  [
            call.initialize(),
            call.get_my_id(),
            # check if we have a db identifier set
            call.dcs_get_database_identifier(),
            # no, ok, init our db
            call.postgresql_initdb(),
            # make sure it starts
            call.postgresql_start(),
            call.postgresql_get_database_identifier(),
            # lock the database identifier so no-one else gets here
            call.dcs_lock('database_identifier')
            ]
    # shut down cleanly
    assert timeout == 5

def test_replica_bootstrap(app):
    app, plugins = app
    setup_plugins(
            plugins,
            dcs_get_database_identifier='1234',
            postgresql_get_database_identifier='42')
    timeout = app.initialize()
    assert app._plugins.mock_calls ==  [
            call.initialize(),
            call.get_my_id(),
            # compare our id with the id in the DCS
            call.dcs_get_database_identifier(),
            call.postgresql_get_database_identifier(),
            # make sure postgresql is stopped
            call.postgresql_stop(),
            # postgresql restore
            call.postgresql_restore(),
            call.postgresql_am_i_replica()
            ]
    # shut down cleanly and immediately
    assert timeout == 0

@pytest.mark.asyncio
async def test_master_start(app):
    app, plugins = app
    setup_plugins(
            plugins,
            dcs_get_database_identifier='1234',
            dcs_lock=True,
            postgresql_am_i_replica=False,
            postgresql_get_database_identifier='1234')
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
            call.postgresql_get_database_identifier(),
            # check if I am a replica
            call.postgresql_am_i_replica(),
            # no, so check if there is a master
            call.dcs_lock('master'),
            # no master, so sure the DB is running
            call.postgresql_start(),
            # start monitoring
            call.start_monitoring()
            ]
    # Carry on running afterwards
    assert timeout == None
    assert app.health_problems == {'test_monitor': {'can_be_replica': False, 'reason': 'Waiting for first check'}}
    # Our test monitor becomes healthy
    plugins.reset_mock()
    app.healthy('test_monitor')
    assert plugins.mock_calls ==  [
            call.postgresql_am_i_replica(),
            call.dcs_lock('master'),
            call.dcs_set_info('conn', {}),
           ] 

def test_replica_start(app):
    app, plugins = app
    setup_plugins(
            plugins,
            dcs_get_database_identifier='1234',
            dcs_lock=True,
            postgresql_am_i_replica=True,
            postgresql_get_database_identifier='1234')
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
            call.postgresql_get_database_identifier(),
            # check if I am a replica
            call.postgresql_am_i_replica(),
            # not master, so sure the DB is running
            call.postgresql_start(),
            # start monitoring
            call.start_monitoring()
            ]
    # Carry on running afterwards
    assert timeout == None
    assert app.health_problems == {'test_monitor': {'can_be_replica': False, 'reason': 'Waiting for first check'}}
    # Our test monitor becomes healthy
    plugins.reset_mock()
    app.healthy('test_monitor')
    assert plugins.mock_calls ==  [
            call.postgresql_am_i_replica(),
            call.dcs_set_info('conn', {'a': 'b'}),
           ] 

def test_restart_master(app):
    app, plugins = app
    setup_plugins(plugins,
            postgresql_am_i_replica=False)
    app.initialize()
    app._plugins.reset_mock()
    with patch('time.sleep') as sleep:
        with patch('sys.exit') as exit:
            app.restart(10)
            assert exit.called_once_with(0)
        assert sleep.called_once_with(10)
    assert app._plugins.mock_calls ==  [
            call.postgresql_am_i_replica(),
            call.postgresql_stop(),
            call.dcs_disconnect()
            ]

def test_restart_replica(app):
    app, plugins = app
    setup_plugins(plugins,
            postgresql_am_i_replica=True)
    app.initialize()
    app._plugins.reset_mock()
    with patch('time.sleep') as sleep:
        with patch('sys.exit') as exit:
            app.restart(10)
            assert exit.called_once_with(0)
        assert sleep.called_once_with(10)
    assert app._plugins.mock_calls ==  [
            call.postgresql_am_i_replica(),
            call.dcs_disconnect()
            ]

@pytest.mark.asyncio
async def test_master_lock_broken(app):
    app, plugins = app
    setup_plugins(plugins,
            postgresql_am_i_replica=False)
    assert app.initialize() == None
    app._plugins.reset_mock()
    # if the lock is broken, shutdown postgresql and exist
    with patch('time.sleep') as sleep:
        with patch('sys.exit') as exit:
            app.master_lock_changed(None)
            assert exit.called_once_with(0)
        assert sleep.called_once_with(10)
    assert app._plugins.mock_calls ==  [
            call.postgresql_am_i_replica(),
            call.postgresql_am_i_replica(),
            call.postgresql_stop(),
            call.dcs_disconnect()
            ]
    assert app._master_lock_owner == None
    # if the lock changes owner to someone else, shutdown postgresql and exist
    app._plugins.reset_mock()
    with patch('time.sleep') as sleep:
        with patch('sys.exit') as exit:
            app.master_lock_changed('someone else')
            assert exit.called_once_with(0)
        assert sleep.called_once_with(10)
    assert app._plugins.mock_calls ==  [
            call.postgresql_am_i_replica(),
            call.postgresql_am_i_replica(),
            call.postgresql_stop(),
            call.dcs_disconnect()
            ]
    assert app._master_lock_owner == 'someone else'
    # if the lock is owned by us, carry on trucking
    app._plugins.reset_mock()
    with patch('time.sleep') as sleep:
        with patch('sys.exit') as exit:
            app.master_lock_changed(app.my_id)
            assert exit.called_once_with(0)
        assert sleep.called_once_with(10)
    assert app._plugins.mock_calls ==  [
            call.postgresql_am_i_replica(),
            ]
    assert app._master_lock_owner == app.my_id

@pytest.mark.asyncio
async def test_replica_reaction_to_master_lock_change(app):
    app, plugins = app
    setup_plugins(plugins,
            postgresql_am_i_replica=True)
    assert app.initialize() == None
    app._plugins.reset_mock()
    # if the lock changes owner to someone else, carry on trucking
    app._plugins.reset_mock()
    app.master_lock_changed('someone else')
    assert app._plugins.mock_calls ==  [
            call.postgresql_am_i_replica(),
            ]
    assert app._master_lock_owner == 'someone else'
    # if the lock is owned by us, er, we stop replication and become the master
    app._plugins.reset_mock()
    app.master_lock_changed(app.my_id)
    assert app._plugins.mock_calls ==  [
            call.postgresql_am_i_replica(),
            call.postgresql_stop_replication(),
            ]
    assert app._master_lock_owner == app.my_id

@pytest.mark.asyncio
async def test_replica_tries_to_take_over(app):
    app, plugins = app
    setup_plugins(plugins,
            postgresql_am_i_replica=True)
    assert app.initialize() == None
    app._plugins.reset_mock()
    # if there is no lock owner, we start looping trying to become master
    app.master_lock_changed(None)
    assert app._plugins.mock_calls ==  [call.postgresql_am_i_replica()]
    app._plugins.reset_mock()
    from asyncio import sleep as real_sleep
    with patch('asyncio.sleep') as sleep:
        sleeper = FakeSleeper()
        sleep.side_effect = sleeper
        await real_sleep(0.001)
        assert sleeper.log == [3]
    assert app._plugins.mock_calls ==  [call.dcs_lock('master')]
