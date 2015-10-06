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

def test_locks(plugin):
    pluginA = plugin()
    pluginA.app.my_id = 'A'
    pluginB = plugin()
    pluginB.app.my_id = 'B'
    assert pluginA.dcs_lock('mylock') == True
    assert pluginB.dcs_lock('mylock') == False
    assert pluginB.dcs_lock('yourlock') == True
    assert pluginA.dcs_lock('yourlock') == False
    assert pluginB.dcs_get_lock_owner('mylock') == 'A'
    assert pluginA.dcs_get_lock_owner('mylock') == 'A'
    assert pluginB.dcs_get_lock_owner('yourlock') == 'B'
    assert pluginA.dcs_get_lock_owner('yourlock') == 'B'
    pluginA.dcs_unlock('mylock')
    assert pluginB.dcs_lock('mylock') == True
    assert pluginA.dcs_lock('mylock') == False
    assert pluginB.dcs_get_lock_owner('mylock') == 'B'
    assert pluginA.dcs_get_lock_owner('mylock') == 'B'
