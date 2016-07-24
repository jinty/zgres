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
def pluginA(plugin):
    return plugin(my_id='A')

@pytest.fixture
def pluginB(plugin):
    return plugin(my_id='B')

@pytest.fixture
def pluginC(plugin):
    return plugin(my_id='C')

@pytest.fixture
def pluginAB(pluginA, pluginB):
    return pluginA, pluginB

@pytest.fixture
def pmA(pluginA):
    from zgres.plugin import get_plugin_manager
    from zgres import deadman
    return get_plugin_manager([('A', pluginA)], deadman).hook

@pytest.fixture
def pmB(pluginB):
    from zgres.plugin import get_plugin_manager
    from zgres import deadman
    return get_plugin_manager([('B', pluginB)], deadman).hook

@pytest.fixture
def pmC(pluginC):
    from zgres.plugin import get_plugin_manager
    from zgres import deadman
    return get_plugin_manager([pluginC], deadman).hook

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

def test_I_can_break_my_own_lock(plugin):
    # a second plugin with a second connection cannot re-lock the same lock
    pluginA1 = plugin(my_id='A')
    pluginA2 = plugin(my_id='A')
    pluginB = plugin(my_id='B')
    assert pluginA1.dcs_lock('mylock') == True
    assert pluginA2.dcs_lock('mylock') == True
    # A2's lock is persistent even on disconnect of A1
    pluginA1.dcs_disconnect()
    assert pluginB.dcs_get_lock_owner('mylock') == 'A'
    assert pluginB.dcs_lock('mylock') == False
    assert pluginA2.dcs_lock('mylock') == True
    assert pluginA2.dcs_lock('mylock') == True

def test_dont_unlock_others(pluginAB):
    pluginA, pluginB = pluginAB
    assert pluginA.dcs_lock('mylock') == True
    pluginB.dcs_unlock('mylock')
    assert pluginB.dcs_lock('mylock') == False

def test_set_delete_conn_info(pluginAB):
    pluginA, pluginB = pluginAB
    pluginA.dcs_set_conn_info(dict(answer=42))
    assert sorted(pluginA.dcs_list_conn_info()) == [('A', dict(answer=42))]
    pluginB.dcs_set_conn_info(dict(answer='X'))
    assert sorted(pluginA.dcs_list_conn_info()) == [('A', dict(answer=42)), ('B', dict(answer='X'))]
    assert sorted(pluginB.dcs_list_conn_info()) == [('A', dict(answer=42)), ('B', dict(answer='X'))]
    assert sorted(pluginA.dcs_list_state()) == []
    pluginA.dcs_delete_conn_info()
    assert sorted(pluginB.dcs_list_conn_info()) == [('B', dict(answer='X'))]

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
    assert sorted(pluginA.dcs_list_conn_info()) == [('A', dict(server=42))]
    assert sorted(pluginA2.dcs_list_conn_info()) == [('A', dict(server=42))]
    pluginA.dcs_disconnect()
    assert sorted(pluginA2.dcs_list_conn_info()) == []

@pytest.mark.asyncio
async def test_master_lock_notification(pmA, pmB):
    master_lock_watcher = mock.Mock()
    pmB.dcs_watch(master_lock=master_lock_watcher, conn_info=None, state=None)
    await asyncio.sleep(0.001)
    pmA.dcs_lock(name='master') # A
    await asyncio.sleep(0.001)
    pmA.dcs_lock(name='master') # no-op
    await asyncio.sleep(0.001)
    pmA.dcs_unlock(name='master') # None
    await asyncio.sleep(0.001)
    pmA.dcs_unlock(name='master') # no-op
    await asyncio.sleep(0.001)
    pmA.dcs_lock(name='master') # A
    await asyncio.sleep(0.001)
    pmA.dcs_unlock(name='master') # None
    await asyncio.sleep(0.001)
    pmB.dcs_lock(name='master') # B
    await asyncio.sleep(0.001)
    pmB.dcs_unlock(name='master') # None
    await asyncio.sleep(0.001)
    assert master_lock_watcher.mock_calls == [
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
async def test_notifications_of_state_chagnges(pmA, pmB):
    # pmB watches state, plugin A doesn't
    pmA.dcs_watch(master_lock=None, conn_info=None, state=None)
    callbackB = mock.Mock()
    pmB.dcs_watch(master_lock=None, conn_info=None, state=callbackB)
    # set state from both plugins
    pmA.dcs_set_state(state=dict(name='A'))
    pmB.dcs_set_state(state=dict(name='B'))
    await asyncio.sleep(0.005)
    # pmB gets events, but ONLY from plugins in its group
    # i.e. c is ignored
    # NOTE: we test only the LAST call as state for A and B may come out-of-order
    #       but the final, rest state, should be correct
    assert callbackB.mock_calls[-1] == mock.call({'A': {'name': 'A'}, 'B': {'name': 'B'}})
    # if we ask for the state, we get the same result
    assert sorted(pmA.dcs_list_state()) == sorted(pmB.dcs_list_state())
    assert sorted(pmA.dcs_list_state()) == [('A', {'name': 'A'}), ('B', {'name': 'B'})]

@pytest.mark.asyncio
async def test_notifications_of_conn_chagnges(pmA, pmB):
    # pluginB watches conn, plugin A doesn't
    pmA.dcs_watch(master_lock=None, conn_info=None, state=None)
    callbackB = mock.Mock()
    pmB.dcs_watch(master_lock=None, conn_info=callbackB, state=None)
    # set conn from both plugins
    pmA.dcs_set_conn_info(conn_info=dict(name='A'))
    pmB.dcs_set_conn_info(conn_info=dict(name='B'))
    await asyncio.sleep(0.005) #sigh, the DCS may use threading, give that a chance
    # pmB gets events, but ONLY from plugins in its group
    # i.e. c is ignored
    # NOTE: we test only the LAST call as conn for A and B may come out-of-order
    #       but the final, rest conn, should be correct
    assert callbackB.mock_calls[-1] == mock.call({'A': {'name': 'A'}, 'B': {'name': 'B'}})
    assert sorted(pmA.dcs_list_conn_info()) == sorted(pmB.dcs_list_conn_info())
    assert sorted(pmA.dcs_list_conn_info()) == [('A', {'name': 'A'}), ('B', {'name': 'B'})]

def test_info_set_from_another_plugin_ephemeral(plugin):
    # 2 servers with the same id should NOT happen in real life...
    pluginA1 = plugin(my_id='A')
    pluginA2 = plugin(my_id='A')
    pluginA3 = plugin(my_id='A')
    # 2 plugins set the same info, but the first gets
    # disconnected after the second has already set the info
    pluginA1.dcs_set_conn_info(dict(server=41))
    pluginA2.dcs_set_conn_info(dict(server=42)) 
    pluginA1.dcs_disconnect()
    # in this case, the info should still be set
    assert sorted(pluginA2.dcs_list_conn_info()) == [('A', dict(server=42))]
    assert sorted(pluginA3.dcs_list_conn_info()) == [('A', dict(server=42))]
