"""Abstract tests for all DCS plugins"""
import pytest

@pytest.fixture(params=['zookeeper'])
def plugin(request):
    if request.param == 'zookeeper':
        from .test_zookeeper import deadman_plugin
        return deadman_plugin()

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
    pluginA._disconnect()
    assert pluginB.dcs_get_database_identifier() == '42'

def test_locks(pluginAB):
    pluginA, pluginB = pluginAB
    # locks are empty by default
    assert pluginB.dcs_get_lock_owner('mylock') is None
    # if one plugin takes one, no others can
    assert pluginA.dcs_lock('mylock') == True
    assert pluginB.dcs_lock('mylock') == False
    assert pluginB.dcs_lock('yourlock') == True
    assert pluginA.dcs_lock('yourlock') == False
    # we can see the owner of the lock
    assert pluginB.dcs_get_lock_owner('mylock') == 'A'
    assert pluginA.dcs_get_lock_owner('mylock') == 'A'
    assert pluginB.dcs_get_lock_owner('yourlock') == 'B'
    assert pluginA.dcs_get_lock_owner('yourlock') == 'B'
    # and we can unlock them
    pluginA.dcs_unlock('mylock')
    assert pluginB.dcs_lock('mylock') == True
    assert pluginA.dcs_lock('mylock') == False
    assert pluginB.dcs_get_lock_owner('mylock') == 'B'
    assert pluginA.dcs_get_lock_owner('mylock') == 'B'

def test_locks_are_ephemeral(pluginAB):
    pluginA, pluginB = pluginAB
    # locks are not persistent
    assert pluginB.dcs_lock('mylock') == True
    assert pluginA.dcs_get_lock_owner('mylock') == 'B'
    pluginB._disconnect()
    assert pluginA.dcs_get_lock_owner('mylock') is None

def test_locks_idempotency(plugin):
    pluginA = plugin()
    pluginA.app.my_id = 'A'
    assert pluginA.dcs_get_lock_owner('mylock') == None
    assert pluginA.dcs_lock('mylock') == True
    assert pluginA.dcs_lock('mylock') == True
    assert pluginA.dcs_get_lock_owner('mylock') == 'A'
    pluginA.dcs_unlock('mylock')
    pluginA.dcs_unlock('mylock')
    assert pluginA.dcs_get_lock_owner('mylock') == None

def test_dont_unlock_others(pluginAB):
    pluginA, pluginB = pluginAB
    assert pluginA.dcs_lock('mylock') == True
    pluginB.dcs_unlock('mylock')
    assert pluginA.dcs_get_lock_owner('mylock') == 'A'

def test_set_delete_info(pluginAB):
    pluginA, pluginB = pluginAB
    pluginA.dcs_set_info('conn', dict(server='A'))
    assert pluginA.dcs_get_info('conn') == dict(server='A')
    pluginA.dcs_set_info('conn', dict(server='B'))
    assert pluginA.dcs_get_info('conn') == dict(server='B')
    assert pluginB.dcs_get_info('conn') == None
    assert pluginA.dcs_get_info('another') == None

def test_info_is_ephemeral(plugin):
    # 2 servers with the same id should NOT happen in real life...
    pluginA = plugin(my_id='A')
    pluginA2 = plugin(my_id='A')
    pluginA.dcs_set_info('conn', dict(server='A'))
    assert pluginA.dcs_get_info('conn') == dict(server='A')
    assert pluginA2.dcs_get_info('conn') == dict(server='A')
    pluginA._disconnect()
    assert pluginA2.dcs_get_info('conn') == None
