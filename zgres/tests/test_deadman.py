import pytest
from unittest.mock import call, patch, Mock

@pytest.fixture
def app():
    from ..deadman import App, _PLUGIN_API
    with patch('zgres._plugin.get_plugins') as get_plugins:
        get_plugins.return_value = Mock(spec_set=[s['name'] for s in _PLUGIN_API])
        app = App({})
    plugins = app._plugins
    return app, app._plugins

def setup_plugins(plugins, **kw):
    from ..deadman import _PLUGIN_API
    defaults = {
            'dcs_get_database_identifier': '12345',
            'postgresql_get_database_identifier': '12345',
            }
    for k, v in kw.items():
        for i in _PLUGIN_API:
            if k == i['name']:
                if i['type'] == 'multiple':
                    # assert that our test plugin data is really unpackable
                    z = [(i, k) for i, k in v]
                    v = iter(z) # make it a real iterable
                break
        getattr(plugins, k).return_value = v

def test_master_bootstrap(app):
    app, plugins = app
    setup_plugins(
            plugins,
            dcs_get_database_identifier=None,
            dcs_lock_database_identifier=True,
            postgresql_get_database_identifier='42')
    assert app.initialize() == 0
    assert app._plugins.mock_calls ==  [
            call.initialize(),
            call.dcs_get_database_identifier(),
            call.postgresql_initdb(),
            call.dcs_lock_database_identifier(),
            call.postgresql_start(),
            call.postgresql_get_database_identifier(),
            call.postgresql_backup(),
            call.dcs_set_database_identifier('42')
            ]
