from configparser import ConfigParser
from unittest import mock
import json
import asyncio

import pytest
from zake.fake_client import FakeClient
from kazoo.client import KazooState

from zgres import sync
from . import FakeSleeper

@pytest.mark.asyncio
async def test_functional():
    """Test as much of the whole stack as we can.
    
    got a nasty sleep(0.1) in it. there should only BE ONE of these tests! the
    others should be real unit tests.
    """
    config = {'sync': {
        'plugins': 'zgres#zgres-apply,zgres#zookeeper',
        'zookeeper': {
            'connection_string': 'example.org:2181',
            'path': '/databases',
            }
        }}
    zk = FakeClient()
    zk.start()
    zk.create("/databases")
    zk.create("/databases/clusterA_conn_10.0.0.2", json.dumps({"node": 1}).encode('utf-8'))
    zk.stop()
    with mock.patch('zgres.apply.Plugin.conn_info') as conn_info:
        with mock.patch('zgres.zookeeper.KazooClient') as KazooClient:
            KazooClient.return_value = zk
            app = sync.SyncApp(config)
    zk.create("/databases/clusterA_conn_10.0.0.1", json.dumps({"node": 1}).encode('utf-8'))
    await asyncio.sleep(0.25)
    # did our state get updated?
    assert dict(app._plugins.plugins['zgres#zookeeper'].watcher) == {
            'clusterA_conn_10.0.0.1': {'node': 1},
            'clusterA_conn_10.0.0.2': {'node': 1},
            }
    # the plugin was called twice, once with the original data, and once with new data
    conn_info.assert_has_calls(
            [mock.call({'clusterA': {'nodes': {'10.0.0.2': {'node': 1}}}}),
                mock.call({'clusterA': {'nodes': {'10.0.0.2': {'node': 1}, '10.0.0.1': {'node': 1}}}})]
            )

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
        zk = FakeClient(storage=storage)
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
        plugin.app.unhealthy.side_effect = lambda *a, **kw: sleeper.finished.set()
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
