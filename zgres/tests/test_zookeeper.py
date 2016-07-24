from configparser import ConfigParser
from unittest import mock
import time
import json
import asyncio

import pytest
from zake.fake_client import FakeClient
from kazoo.client import KazooState
import kazoo.exceptions

from zgres import sync
from . import FakeSleeper

class MyFakeClient(FakeClient):

    @property
    def client_id(self):
        return (self.session_id, 'abc')

@pytest.mark.asyncio
async def test_functional(deadman_plugin):
    """Test as much of the whole stack of zgres-sync as we can."""
    config = {
            'sync': {
                'plugins': 'zgres#zookeeper zgres#mock-subscriber'},
            'zookeeper': {
                'connection_string': 'example.org:2181',
                'path': '/mypath',
                }
            }
    deadmanA = deadman_plugin('A')
    deadmanB = deadman_plugin('B')
    deadmanA.dcs_set_database_identifier('1234')
    deadmanA.dcs_set_conn_info(dict(answer=42))
    deadmanA.dcs_lock('master')
    deadmanB.dcs_set_state(dict(mystate='lamentable'))
    ev = asyncio.Event()
    async def next_ev():
        await ev.wait()
        ev.clear()
    def set_ev(*args, **kw):
        ev.set()
    for i in range(10):
        asyncio.get_event_loop().call_later(4 + 0.1 * i, set_ev)
    from . import MockSyncPlugin as RealMockSyncPlugin
    with mock.patch('zgres.tests.MockSyncPlugin') as MockSyncPlugin:
        # sigh, FAR to complex
        proxy, p = RealMockSyncPlugin('', '')
        p.databases.side_effect = set_ev
        p.state.side_effect = set_ev
        p.masters.side_effect = set_ev
        p.conn_info.side_effect = set_ev
        MockSyncPlugin.return_value = proxy
        with mock.patch('zgres.zookeeper.KazooClient') as KazooClient:
            KazooClient.return_value = MyFakeClient(storage=deadmanA._storage._zk._storage)
            app = sync.SyncApp(config)
    for i in range(3):
        await next_ev()
    deadmanA.dcs_set_state(dict(mystate='great!'))
    deadmanB.dcs_set_conn_info(dict(answer=43))
    deadmanA.dcs_unlock('master')
    for i in range(3):
        await next_ev()
    # the plugin was called twice, once with the original data, and once with new data
    assert p.conn_info.mock_calls == [
            mock.call({'mygroup': {'A': {'answer': 42}}}),
            mock.call({'mygroup': {'A': {'answer': 42}, 'B': {'answer': 43}}})]
    p.state.assert_has_calls(
            [mock.call({'mygroup': {'B': {'mystate': 'lamentable'}}}),
                mock.call({'mygroup': {'B': {'mystate': 'lamentable'}, 'A': {'mystate': 'great!'}}})]
            )
    p.masters.assert_has_calls(
            [mock.call({'mygroup': 'A'}),
                mock.call({})]
            )
    p.databases.assert_has_calls([mock.call(['mygroup'])])

@pytest.fixture
def storage(request):
    from ..zookeeper import ZookeeperStorage
    s = ZookeeperStorage('connection_string', '/path')
    zk = MyFakeClient()
    with mock.patch('zgres.zookeeper.KazooClient') as KazooClient:
        KazooClient.return_value = zk
        s.dcs_connect()
    return s

@pytest.fixture
def deadman_plugin(request):
    from ..deadman import App
    storage = None
    def factory(my_id='42'):
        nonlocal storage
        app = mock.Mock(spec_set=App)
        app.my_id = my_id
        app.restart._is_coroutine = False
        app.config = dict(
                zookeeper=dict(
                    connection_string='localhost:1234',
                    path='/mypath',
                    group='mygroup',
                    ))
        app.master_lock_changed._is_coroutine = False # otherwise tests fail :(
        from ..zookeeper import ZooKeeperDeadmanPlugin
        plugin = ZooKeeperDeadmanPlugin('zgres#zookeeper', app)
        zk = MyFakeClient(storage=storage)
        if storage is None:
            # all plugins created by this factory SHARE a storage
            storage = zk.storage
        with mock.patch('zgres.zookeeper.KazooClient') as KazooClient:
            KazooClient.return_value = zk
            plugin.initialize()
        return plugin
    return factory

@pytest.mark.asyncio
async def test_disconnect_should_not_restart(deadman_plugin):
    plugin = deadman_plugin()
    await asyncio.sleep(0.001)
    plugin.dcs_disconnect()
    await asyncio.sleep(0.001)
    assert plugin.app.mock_calls == [] # restart was not called

@pytest.mark.asyncio
async def test_session_suspended(deadman_plugin):
    plugin = deadman_plugin()
    await asyncio.sleep(0.001)
    # suspend the connection
    plugin.logger.warn = mock.Mock()
    plugin._storage._zk._fire_state_change(KazooState.SUSPENDED)
    await asyncio.sleep(0.001)
    plugin.logger.warn.assert_called_once_with('zookeeper connection state: SUSPENDED')
    assert plugin._dcs_state == 'SUSPENDED'
    assert plugin.app.mock_calls == []

@pytest.mark.asyncio
async def test_session_suspended_but_reconnect_in_5_seconds(deadman_plugin):
    with mock.patch('zgres.zookeeper.sleep') as sleep:
        # yeah, tests with firewalls show that this really does happen
        plugin = deadman_plugin()
        await asyncio.sleep(0.001)
        sleeper = FakeSleeper(max_loops=1000)
        sleep.side_effect = sleeper
        # suspend the connection
        plugin.logger.warn = mock.Mock()
        plugin._storage._zk._fire_state_change(KazooState.SUSPENDED)
        await sleeper.next()
        await sleeper.next()
        await sleeper.next()
        await sleeper.next()
        ntasks = len(asyncio.Task.all_tasks())
        plugin._storage._zk._fire_state_change(KazooState.CONNECTED)
        time.sleep(0.001)
        await asyncio.sleep(0.001)
        assert ntasks - len(asyncio.Task.all_tasks()) == 1 # the _check_state task finished 
        assert plugin.app.mock_calls == []
        assert plugin.logger.warn.mock_calls == [
                mock.call('zookeeper connection state: SUSPENDED'),
                mock.call('zookeeper connection state: CONNECTED'),
                ]
        assert plugin._dcs_state == KazooState.CONNECTED

@pytest.mark.asyncio
async def test_session_suspended_but_never_reconnects_or_is_lost(deadman_plugin):
    with mock.patch('zgres.zookeeper.sleep') as sleep:
        # yeah, tests with firewalls show that this really does happen
        plugin = deadman_plugin()
        await asyncio.sleep(0.001)
        sleeper = FakeSleeper(max_loops=25)
        sleep.side_effect = sleeper
        def finish(timeout):
            sleeper.finish()
        plugin.app.restart.side_effect = finish
        # suspend the connection
        plugin.logger.warn = mock.Mock()
        plugin._storage._zk._fire_state_change(KazooState.SUSPENDED)
        await sleeper.wait()
        assert plugin.app.mock_calls == [
                mock.call.restart(0)
                ]
        assert plugin.logger.warn.mock_calls == [
                mock.call('zookeeper connection state: SUSPENDED'),
                ]
        assert plugin._dcs_state == KazooState.SUSPENDED

@pytest.mark.asyncio
async def test_session_lost(deadman_plugin):
    plugin = deadman_plugin()
    await asyncio.sleep(0.001)
    plugin.app.reset_mock()
    plugin._storage._zk._fire_state_change(KazooState.LOST)
    await asyncio.sleep(0.001)
    assert plugin._dcs_state == 'LOST'
    assert plugin.app.mock_calls == [
            mock.call.restart(0)
            ]

@pytest.mark.asyncio
async def test_notifications_of_state_chagnge_where_id_has_a_dash(deadman_plugin):
    pluginA = deadman_plugin('i-9b61354f')
    finished = asyncio.Event()
    asyncio.get_event_loop().call_later(5, finished.set)
    callback = mock.Mock()
    callback.side_effect = lambda *args, **kw: finished.set()
    pluginA.dcs_watch(None, callback, None)
    pluginA.dcs_set_state(dict(name='A'))
    await finished.wait()
    assert callback.mock_calls == [
            mock.call({'i-9b61354f': {'name': 'A'}}),
            ]

@pytest.mark.asyncio
async def test_groups_are_independant(deadman_plugin):
    plugin = deadman_plugin
    pluginA, pluginB, pluginC = plugin('A'), plugin('B'), plugin('C')
    pluginC._group_name = 'another'
    # pluginB watches state, plugin A doesn't
    pluginA.dcs_watch(None, None, None)
    callbackB = mock.Mock()
    pluginB.dcs_watch(None, callbackB, None)
    callbackC = mock.Mock()
    pluginC.dcs_watch(None, callbackC, None)
    # set state from both plugins
    pluginA.dcs_set_state(dict(name='A'))
    pluginB.dcs_set_state(dict(name='B'))
    pluginC.dcs_set_state(dict(name='C'))
    await asyncio.sleep(0.005)
    # pluginB gets events, but ONLY from plugins in its group
    # i.e. c is ignored
    # NOTE: we test only the LAST call as state for A and B may come out-of-order
    #       but the final, rest state, should be correct
    assert callbackB.mock_calls[-1] == mock.call({'A': {'name': 'A'}, 'B': {'name': 'B'}})
    # C got it's own event
    assert callbackC.mock_calls == [
            mock.call({'C': {'name': 'C'}}),
            ]
    # We can get all info
    assert sorted(pluginA.dcs_list_state()) == sorted(pluginB.dcs_list_state())
    assert sorted(pluginA.dcs_list_state()) == [('A', {'name': 'A'}), ('B', {'name': 'B'})]
    assert sorted(pluginC.dcs_list_state()) == [('C', {'name': 'C'})]

def test_errorlog_after_second_takeover(deadman_plugin):
    plugin = deadman_plugin
    # 2 servers with the same id should NOT happen in real life...
    pluginA1 = plugin(my_id='A')
    pluginA2 = plugin(my_id='A')
    pluginA2.logger = mock.Mock()
    # now they start to fight
    pluginA1.dcs_set_state(dict(server=41))
    pluginA2.dcs_set_state(dict(server=42)) 
    pluginA1.dcs_set_state(dict(server=43))
    # this is the second time plugin2 is taking over
    # We should log an error message now
    assert not pluginA2.logger.error.called
    pluginA2.dcs_set_state(dict(server=44)) 
    assert pluginA2.logger.error.called
    # though the state is still set
    assert sorted(pluginA1.dcs_list_state()) == [('A', dict(server=44))]

def test_storage_get_database_identifiers(storage):
    assert storage.dcs_get_database_identifiers() == {}
    storage.dcs_set_database_identifier('db1', '124')
    assert storage.dcs_get_database_identifiers() == {'db1': 124}

def mock_verify(plugin, side_effect):
    # cause the verify() function to fail in zake, thus all api calls error
    verify = mock.Mock()
    verify.side_effect = side_effect
    plugin._storage.connection.verify = verify
    plugin._kazoo_retry.sleep_func = lambda x: None # speed up tests by not sleeping
    return verify

@pytest.mark.asyncio
async def test_retry_on_connection_loss(deadman_plugin):
    # connection loss is a temporary exception which seems to happen after a re-connection
    # (but not session expiration)in zookeeper. We just retry that till it works.
    plugin = deadman_plugin('A')
    verify = mock_verify(plugin, [
        kazoo.exceptions.ConnectionLoss(),
        kazoo.exceptions.ConnectionLoss(),
        kazoo.exceptions.ConnectionLoss(),
        kazoo.exceptions.ConnectionLoss(),
        kazoo.exceptions.ConnectionLoss(),
        None,
        None])
    # set state from both plugins
    plugin.dcs_set_state(dict(name='A'))
    await asyncio.sleep(0.001)
    assert plugin.app.mock_calls == []
    assert verify.call_count > 4

@pytest.mark.asyncio
async def test_retry_NO_retry_on_session_expired(deadman_plugin):
    # connection loss is a temporary exception which seems to happen after a re-connection
    # (but not session expiration)in zookeeper. We just retry that till it works.
    plugin = deadman_plugin('A')
    verify = mock_verify(plugin, [kazoo.exceptions.SessionExpiredError()])
    # set state from both plugins
    with pytest.raises(kazoo.exceptions.SessionExpiredError):
        plugin.dcs_set_state(dict(name='A'))
    await asyncio.sleep(0.001)
    assert plugin.app.mock_calls == [
            mock.call.restart(0)
            ]

@pytest.mark.asyncio
async def test_retry_with_random_exception(deadman_plugin):
    # connection loss is a temporary exception which seems to happen after a re-connection
    # (but not session expiration)in zookeeper. We just retry that till it works.
    plugin = deadman_plugin('A')
    class MyException(Exception):
        pass
    verify = mock_verify(plugin, [MyException()])
    # set state from both plugins
    with pytest.raises(MyException):
        plugin.dcs_set_state(dict(name='A'))
    await asyncio.sleep(0.001)
    assert plugin.app.mock_calls == []

import time as ttt

@pytest.mark.asyncio
async def test_retry_deadline(deadman_plugin):
    with mock.patch('time.time') as time:
        plugin = deadman_plugin('A')
        time.return_value = 120
        print(ttt.time(), time())
        def my_side_effect():
            time.return_value = 240
            raise kazoo.exceptions.ConnectionLoss()
        verify = mock_verify(plugin, my_side_effect)
        # set state from both plugins
        with pytest.raises(kazoo.retry.RetryFailedError) as e:
            plugin.dcs_set_state(dict(name='A'))
    assert e.value.args[0] == "Exceeded retry deadline"
    await asyncio.sleep(0.001)
    assert plugin.app.mock_calls == []

@pytest.mark.asyncio
async def test_retry_list_all_states(deadman_plugin):
    # connection loss is a temporary exception which seems to happen after a re-connection
    # (but not session expiration)in zookeeper. We just retry that till it works.
    plugin = deadman_plugin('A')
    plugin.dcs_set_state(dict(name='A'))
    verify = mock_verify(plugin, [
        kazoo.exceptions.ConnectionLoss(),
        kazoo.exceptions.ConnectionLoss(),
        kazoo.exceptions.ConnectionLoss(),
        None,
        None,
        None,
        None])
    # set state from both plugins
    assert list(plugin.dcs_list_state()) == [('A', {'name': 'A'})]
    await asyncio.sleep(0.001)
    assert plugin.app.mock_calls == []

