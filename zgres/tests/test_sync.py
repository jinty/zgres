from configparser import ConfigParser
from unittest import mock
import json
import asyncio
import pytest

from zgres import sync

from zake.fake_client import FakeClient

@pytest.fixture
def zk():
    zk =  FakeClient()
    zk.start()
    return zk

@pytest.mark.asyncio
async def test_functional(zk):
    """Test as much of the whole stack as we can.
    
    got a nasty sleep(0.1) in it. there should only BE ONE of these tests! the
    others should be real unit tests.
    """
    config = ConfigParser()
    config.read_dict({'sync': {
        'zookeeper_connection_string': 'example.org:2181',
        'zookeeper_path': '/databases',
        'plugins': 'zgres-apply',
        }})
    zk.create("/databases")
    zk.create("/databases/clusterA_conn_10.0.0.2", json.dumps({"node": 1}).encode('utf-8'))
    with mock.patch('zgres.apply.Plugin') as Plugin:
        with mock.patch('zgres.sync.KazooClient') as KazooClient:
            KazooClient.return_value = zk
            app = sync.SyncApp(config)
    zk.create("/databases/clusterA_conn_10.0.0.1", json.dumps({"node": 1}).encode('utf-8'))
    await asyncio.sleep(0.25)
    # did our state get updated?
    assert dict(app.source.watcher) == {
            'clusterA_conn_10.0.0.1': {'node': 1},
            'clusterA_conn_10.0.0.2': {'node': 1},
            }
    # the plugin was called twice, once with the original data, and once with new data
    Plugin().conn_info.assert_has_calls(
            [mock.call({'clusterA': {'nodes': {'10.0.0.2': {'node': 1}}}}),
                mock.call({'clusterA': {'nodes': {'10.0.0.2': {'node': 1}, '10.0.0.1': {'node': 1}}}})]
            )

