"""Abstract tests for all DCS plugins"""
import asyncio
from unittest import mock

import pytest

@pytest.fixture(params=['zookeeper'])
def plugin(request):
    if request.param == 'zookeeper':
        from .test_zookeeper import deadman_plugin
        return deadman_plugin(request)

@pytest.fixture
def pluginAB(plugin):
    pluginA = plugin(my_id='A')
    pluginB = plugin(my_id='B')
    return pluginA, pluginB

def test_database_identifier(pluginAB):
    pluginA, pluginB = pluginAB
    # there is no db identifier
    assert pluginA.dcs_get_database_identifier() is None
    # but we can set it (True means success)
    assert pluginA.dcs_set_database_identifier('42') == True
    # we can get it again
    assert pluginA.dcs_get_database_identifier() == '42'
    # setting it again will return False, and NOT set the value
    assert pluginA.dcs_set_database_identifier('42') == False
    assert pluginA.dcs_get_database_identifier() == '42'
    assert pluginA.dcs_set_database_identifier('49') == False
    assert pluginA.dcs_get_database_identifier() == '42'
    # a separate DCS plugin connected to the same storage also sees the same behaviour
    assert pluginB.dcs_set_database_identifier('49') == False
    assert pluginB.dcs_get_database_identifier() == '42'
    # database_identifiers are persistent
    pluginA.dcs_disconnect()
    assert pluginB.dcs_get_database_identifier() == '42'

def test_locks(pluginAB):
    pluginA, pluginB = pluginAB
    # if one plugin takes one, no others can
    assert pluginA.dcs_lock('mylock') == True
    assert pluginB.dcs_lock('mylock') == False
    assert pluginB.dcs_lock('yourlock') == True
    assert pluginA.dcs_lock('yourlock') == False
    # and we can unlock them
    pluginA.dcs_unlock('mylock')
    assert pluginB.dcs_lock('mylock') == True
    assert pluginA.dcs_lock('mylock') == False

def test_locks_are_ephemeral(pluginAB):
    pluginA, pluginB = pluginAB
    # locks are not persistent
    assert pluginB.dcs_lock('mylock') == True
    assert pluginA.dcs_lock('mylock') == False
    pluginB.dcs_disconnect()
    assert pluginA.dcs_lock('mylock') == True

def test_locks_idempotency(pluginAB):
    pluginA, pluginB = pluginAB
    assert pluginA.dcs_lock('mylock') == True
    assert pluginA.dcs_lock('mylock') == True
    assert pluginB.dcs_lock('mylock') == False
    pluginA.dcs_unlock('mylock')
    pluginA.dcs_unlock('mylock')
    assert pluginB.dcs_lock('mylock') == True

def test_locks_are_attached_to_the_connection_NOT_id(plugin):
    # a second plugin with a second connection cannot re-lock the same lock
    pluginA1 = plugin(my_id='A')
    pluginA2 = plugin(my_id='A')
    assert pluginA1.dcs_lock('mylock') == True
    assert pluginA2.dcs_lock('mylock') == False

def test_dont_unlock_others(pluginAB):
    pluginA, pluginB = pluginAB
    assert pluginA.dcs_lock('mylock') == True
    pluginB.dcs_unlock('mylock')
    assert pluginB.dcs_lock('mylock') == False

def test_set_delete_info(pluginAB):
    pluginA, pluginB = pluginAB
    pluginA.dcs_set_info('conn', dict(server='A'))
    assert pluginA.dcs_get_info('conn') == dict(server='A')
    pluginA.dcs_set_info('conn', dict(server='B'))
    assert pluginA.dcs_get_info('conn') == dict(server='B')
    assert pluginB.dcs_get_info('conn') == None
    assert pluginA.dcs_get_info('another') == None
    pluginA.dcs_delete_info('conn')
    assert pluginA.dcs_get_info('conn') == None

def test_set_delete_info_is_idempotent(plugin):
    plugin = plugin()
    plugin.dcs_delete_info('conn')
    plugin.dcs_delete_info('conn')

def test_info_is_ephemeral(plugin):
    # 2 servers with the same id should NOT happen in real life...
    pluginA = plugin(my_id='A')
    pluginA2 = plugin(my_id='A')
    pluginA.dcs_set_info('conn', dict(server='A'))
    assert pluginA.dcs_get_info('conn') == dict(server='A')
    assert pluginA2.dcs_get_info('conn') == dict(server='A')
    pluginA.dcs_disconnect()
    assert pluginA2.dcs_get_info('conn') == None

@pytest.mark.asyncio
async def test_master_lock_notification(plugin):
    pluginA, pluginB = plugin('A'), plugin('B')
    pluginB.start_monitoring() # None
    await asyncio.sleep(0.001)
    pluginA.dcs_lock('master') # A
    await asyncio.sleep(0.001)
    pluginA.dcs_lock('master') # no-op
    await asyncio.sleep(0.001)
    pluginA.dcs_unlock('master') # None
    await asyncio.sleep(0.001)
    pluginA.dcs_unlock('master') # no-op
    await asyncio.sleep(0.001)
    pluginA.dcs_lock('master') # A
    await asyncio.sleep(0.001)
    pluginA.dcs_unlock('master') # None
    await asyncio.sleep(0.001)
    pluginB.dcs_lock('master') # B
    await asyncio.sleep(0.001)
    pluginB.dcs_unlock('master') # None
    await asyncio.sleep(0.001)
    # just a sanity check that these really go through the DCS
    assert pluginB.app is not pluginA.app
    assert pluginB.app.master_lock_changed.mock_calls == [
            mock.call(None),
            mock.call('A'),
            mock.call(None),
            mock.call('A'),
            mock.call(None),
            mock.call('B'),
            mock.call(None),
            ]
