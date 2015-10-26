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

def test_set_delete_conn_info(pluginAB):
    pluginA, pluginB = pluginAB
    pluginA.dcs_set_conn_info(dict(answer=42))
    assert sorted(pluginA.dcs_get_all_conn_info()) == [('A', dict(answer=42))]
    pluginB.dcs_set_conn_info(dict(answer='X'))
    assert sorted(pluginA.dcs_get_all_conn_info()) == [('A', dict(answer=42)), ('B', dict(answer='X'))]
    assert sorted(pluginB.dcs_get_all_conn_info()) == [('A', dict(answer=42)), ('B', dict(answer='X'))]
    assert sorted(pluginA.dcs_get_all_state()) == []
    pluginA.dcs_delete_conn_info()
    assert sorted(pluginB.dcs_get_all_conn_info()) == [('B', dict(answer='X'))]

def test_set_delete_info_is_idempotent(plugin):
    plugin = plugin()
    plugin.dcs_set_conn_info(dict(server=42))
    plugin.dcs_delete_conn_info()
    plugin.dcs_delete_conn_info()

def test_info_is_ephemeral(plugin):
    # 2 servers with the same id should NOT happen in real life...
    pluginA = plugin(my_id='A')
    pluginA2 = plugin(my_id='A')
    pluginA.dcs_set_conn_info(dict(server=42))
    assert sorted(pluginA.dcs_get_all_conn_info()) == [('A', dict(server=42))]
    assert sorted(pluginA2.dcs_get_all_conn_info()) == [('A', dict(server=42))]
    pluginA.dcs_disconnect()
    assert sorted(pluginA2.dcs_get_all_conn_info()) == []

@pytest.mark.asyncio
async def test_master_lock_notification(plugin):
    pluginA, pluginB = plugin('A'), plugin('B')
    pluginB.dcs_watch()
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

def test_timeline_default(plugin):
    assert plugin().dcs_get_timeline() == 0

def test_timelines_are_public(pluginAB):
    pluginA, pluginB = pluginAB
    pluginA.dcs_set_timeline(42)
    assert pluginA.dcs_get_timeline() == 42
    assert pluginB.dcs_get_timeline() == 42
    pluginB.dcs_set_timeline(43)
    assert pluginA.dcs_get_timeline() == 43
    assert pluginB.dcs_get_timeline() == 43

def test_timelines_can_only_increment(pluginAB):
    pluginA, pluginB = pluginAB
    pluginA.dcs_set_timeline(42)
    pluginB.dcs_set_timeline(49)
    with pytest.raises(ValueError) as exec:
        assert pluginB.dcs_set_timeline(42)
    with pytest.raises(ValueError) as exec:
        assert pluginA.dcs_set_timeline(42)
    assert pluginA.dcs_get_timeline() == 49

def test_timelines_persist(pluginAB):
    pluginA, pluginB = pluginAB
    pluginA.dcs_set_timeline(49)
    pluginA.dcs_disconnect()
    assert pluginB.dcs_get_timeline() == 49

@pytest.mark.asyncio
async def test_notifications_of_state_chagnges(plugin):
    pluginA, pluginB = plugin('A'), plugin('B')
    # pluginB watches state, plugin A doesn't
    pluginA.dcs_watch()
    callbackB = mock.Mock()
    pluginB.dcs_watch(state=callbackB)
    # set state from both plugins
    pluginA.dcs_set_state(dict(name='A'))
    pluginB.dcs_set_state(dict(name='B'))
    await asyncio.sleep(0.005)
    # pluginB gets events, but ONLY from plugins in its group
    # i.e. c is ignored
    # NOTE: we test only the LAST call as state for A and B may come out-of-order
    #       but the final, rest state, should be correct
    assert callbackB.mock_calls[-1] == mock.call({'A': {'name': 'A'}, 'B': {'name': 'B'}})
    # if we ask for the state, we get the same result
    assert sorted(pluginA.dcs_get_all_state()) == sorted(pluginB.dcs_get_all_state())
    assert sorted(pluginA.dcs_get_all_state()) == [('A', {'name': 'A'}), ('B', {'name': 'B'})]

@pytest.mark.asyncio
async def test_notifications_of_conn_chagnges(plugin):
    pluginA, pluginB = plugin('A'), plugin('B')
    # pluginB watches conn, plugin A doesn't
    pluginA.dcs_watch()
    callbackB = mock.Mock()
    pluginB.dcs_watch(conn_info=callbackB)
    # set conn from both plugins
    pluginA.dcs_set_conn_info(dict(name='A'))
    pluginB.dcs_set_conn_info(dict(name='B'))
    await asyncio.sleep(0.005) #sigh, the DCS may use threading, give that a chance
    # pluginB gets events, but ONLY from plugins in its group
    # i.e. c is ignored
    # NOTE: we test only the LAST call as conn for A and B may come out-of-order
    #       but the final, rest conn, should be correct
    assert callbackB.mock_calls[-1] == mock.call({'A': {'name': 'A'}, 'B': {'name': 'B'}})
    assert sorted(pluginA.dcs_get_all_conn_info()) == sorted(pluginB.dcs_get_all_conn_info())
    assert sorted(pluginA.dcs_get_all_conn_info()) == [('A', {'name': 'A'}), ('B', {'name': 'B'})]


