from configparser import ConfigParser
from unittest import mock
import json
import asyncio
import pytest

from zgres import sync

from zake.fake_client import FakeClient

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
def deadman_plugin():
    storage = None
    def factory(my_id='42'):
        nonlocal storage
        app = mock.Mock()
        app.my_id = my_id
        app.config = dict(
                zookeeper=dict(
                    connection_string='localhost:1234',
                    path='/mypath',
                    group='mygroup',
                    ))
        from zake.fake_client import FakeClient
        from ..zookeeper import ZooKeeperDeadmanPlugin
        plugin = ZooKeeperDeadmanPlugin('zgres#zookeeper', app)
        zk = FakeClient(storage=storage)
        if storage is None:
            # all plugins created by this factory SHARE a storage
            storage = zk.storage
        with mock.patch('zgres.zookeeper.KazooClient') as KazooClient:
            KazooClient.return_value = zk
            plugin.initialize()
        return plugin
    return factory
