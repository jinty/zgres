from configparser import ConfigParser
from unittest import mock
import json
import asyncio

import pytest
from zake.fake_client import FakeClient
from kazoo.client import KazooState

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
        p = RealMockSyncPlugin('', '')
        p.databases.side_effect = set_ev
        p.state.side_effect = set_ev
        p.masters.side_effect = set_ev
        p.conn_info.side_effect = set_ev
        MockSyncPlugin.return_value = p
        with mock.patch('zgres.zookeeper.KazooClient') as KazooClient:
            KazooClient.return_value = MyFakeClient(storage=deadmanA._zk._storage)
            app = sync.SyncApp(config)
    for i in range(3):
        await next_ev()
    deadmanA.dcs_set_state(dict(mystate='great!'))
    deadmanB.dcs_set_conn_info(dict(answer=43))
    deadmanA.dcs_unlock('master')
    for i in range(3):
        await next_ev()
    # the plugin was called twice, once with the original data, and once with new data
    p.conn_info.assert_has_calls(
            [mock.call({'mygroup': {'A': {'answer': 42}}}),
                mock.call({'mygroup': {'A': {'answer': 42}, 'B': {'answer': 43}}})]
            )
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
def deadman_plugin(request):
    from ..deadman import App
    storage = None
    def factory(my_id='42'):
        nonlocal storage
        app = mock.Mock(spec_set=App)
        app.my_id = my_id
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
        request.addfinalizer(plugin.dcs_disconnect)
        return plugin
    return factory

@pytest.mark.asyncio
async def test_session_suspended(deadman_plugin):
    plugin = deadman_plugin()
    await asyncio.sleep(0.001)
    plugin.app.reset_mock()
    with mock.patch('asyncio.sleep') as sleep:
        sleeper = FakeSleeper(max_loops=2)
        sleep.side_effect = sleeper
        plugin.app.unhealthy.side_effect = lambda *a, **kw: sleeper.finish()
        # suspend the connection
        plugin._zk.state = KazooState.SUSPENDED
        plugin._zk._fire_state_change(KazooState.SUSPENDED)
        await sleeper.wait()
        assert plugin.app.mock_calls == [
                mock.call.unhealthy(
                    'zgres#zookeeper.no_zookeeper_connection',
                    'No connection to zookeeper: SUSPENDED',
                    can_be_replica=True)
                ]
        assert sleeper.log == [plugin.tick_time]

@pytest.mark.asyncio
async def test_session_suspended_but_reconnects(deadman_plugin):
    plugin = deadman_plugin()
    await asyncio.sleep(0.001)
    plugin.app.reset_mock()
    with mock.patch('asyncio.sleep') as sleep:
        sleeper = FakeSleeper()
        sleep.side_effect = sleeper
        # suspend the connection
        plugin._zk._fire_state_change(KazooState.SUSPENDED)
        plugin._zk.state = KazooState.CONNECTED
        await sleeper.next()
        assert sleeper.log == [plugin.tick_time]
    await asyncio.sleep(0.001)
    assert plugin.app.mock_calls == []

@pytest.mark.asyncio
async def test_session_lost(deadman_plugin):
    plugin = deadman_plugin()
    await asyncio.sleep(0.001)
    plugin.app.reset_mock()
    plugin._zk._fire_state_change(KazooState.LOST)
    plugin._zk.state = KazooState.LOST
    await asyncio.sleep(0.001)
    assert plugin.app.mock_calls == [
            mock.call.restart(10)
            ]

@pytest.mark.asyncio
async def test_session_connects(deadman_plugin):
    plugin = deadman_plugin()
    await asyncio.sleep(0.001)
    plugin.app.reset_mock()
    plugin._zk._fire_state_change(KazooState.CONNECTED)
    plugin._zk.state = KazooState.CONNECTED
    await asyncio.sleep(0.001)
    assert plugin.app.mock_calls == [
            mock.call.healthy('zgres#zookeeper.no_zookeeper_connection')
            ]

@pytest.mark.asyncio
async def test_notifications_of_state_chagnge_where_id_has_a_dash(deadman_plugin):
    pluginA = deadman_plugin('i-9b61354f')
    finished = asyncio.Event()
    asyncio.get_event_loop().call_later(5, finished.set)
    callback = mock.Mock()
    callback.side_effect = lambda *args, **kw: finished.set()
    pluginA.dcs_watch(state=callback)
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
    pluginA.dcs_watch()
    callbackB = mock.Mock()
    pluginB.dcs_watch(state=callbackB)
    callbackC = mock.Mock()
    pluginC.dcs_watch(state=callbackC)
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
