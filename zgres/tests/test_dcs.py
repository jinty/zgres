"""Abstract tests for all DCS plugins"""
import pytest

@pytest.fixture(params=['zookeeper'])
def plugin(request):
    if request.param == 'zookeeper':
        from .test_zookeeper import deadman_plugin
        return deadman_plugin()

def test_database_identifier(plugin):
    pluginA = plugin()
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
    pluginB = plugin()
    assert pluginB.dcs_set_database_identifier('49') == False
    assert pluginB.dcs_get_database_identifier() == '42'
    # database_identifiers are persistent
    pluginA._disconnect()
    assert pluginB.dcs_get_database_identifier() == '42'

def test_locks(plugin):
    pluginA = plugin()
    pluginA.app.my_id = 'A'
    pluginB = plugin()
    pluginB.app.my_id = 'B'
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

def test_locks_are_ephemeral(plugin):
    # locks are not persistent
    pluginA = plugin()
    pluginA.app.my_id = 'A'
    pluginB = plugin()
    pluginB.app.my_id = 'B'
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

def test_dont_unlock_others(plugin):
    pluginA = plugin()
    pluginA.app.my_id = 'A'
    pluginB = plugin()
    pluginB.app.my_id = 'B'
    assert pluginA.dcs_lock('mylock') == True
    pluginB.dcs_unlock('mylock')
    assert pluginA.dcs_get_lock_owner('mylock') == 'A'
