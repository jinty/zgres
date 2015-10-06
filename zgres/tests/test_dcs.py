"""Abstract tests for all DCS plugins"""
import pytest

@pytest.fixture(params=['zookeeper'])
def plugin(request):
    if request.param == 'zookeeper':
        from .test_zookeeper import deadman_plugin
        return deadman_plugin()

def test_database_identifier(plugin):
    # there is no db identifier
    assert plugin.dcs_get_database_identifier() is None
    # but we can set it (True means success)
    assert plugin.dcs_set_database_identifier('42') == True
    # we can get it again
    assert plugin.dcs_get_database_identifier() == '42'
    # setting it again will return False, and NOT set the value
    assert plugin.dcs_set_database_identifier('42') == False
    assert plugin.dcs_get_database_identifier() == '42'
    assert plugin.dcs_set_database_identifier('49') == False
    assert plugin.dcs_get_database_identifier() == '42'
