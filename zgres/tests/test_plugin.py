from unittest.mock import patch, Mock
from configparser import ConfigParser

import pytest

from .._plugin import *

def test_real_plugins():
    config = ConfigParser()
    config.read_dict({
        'sync': {
            'zgres.conn': 'zgres-apply',
            }})
    plugins = get_configured_plugins(config, 'sync', 'zgres.conn')
    from zgres.apply import conn_info_plugin
    assert plugins == [('zgres-apply', conn_info_plugin)]

@patch('zgres._plugin.iter_entry_points')
def test_multiple_plugins(iter_entry_points):
    plugin1 = Mock()
    plugin1.name = 'plugin1'
    plugin2 = Mock()
    plugin2.name = 'plugin2'
    plugin3 = Mock()
    plugin3.name = 'plugin3'
    iter_entry_points.return_value = [plugin2, plugin3, plugin1]
    config = ConfigParser()
    config.read_dict({
        'global': {
            'whatever': 'plugin1,plugin2',
            'whatever_reverse': 'plugin2,plugin1',
            'other': 'plugin2,plugin3',
            }})
    plugins = get_configured_plugins(config, 'global', 'whatever')
    plugin1.load.assert_called_once_with(require=False)
    plugin1.load().assert_called_once_with()
    assert plugins == [('plugin1', plugin1.load()()), ('plugin2', plugin2.load()())]
    # reverse order in the config file and you see the plugins are returned in that order
    plugins = get_configured_plugins(config, 'global', 'whatever_reverse')
    assert plugins == [('plugin2', plugin2.load()()), ('plugin1', plugin1.load()())]
    # try now with only one plugin configured
    assert not plugin3.load.called
    plugins = get_configured_plugins(config, 'global', 'other')
    plugin3.load.assert_called_once_with(require=False)
    plugin3.load().assert_called_once_with()
    assert plugins == [('plugin2', plugin2.load()()), ('plugin3', plugin3.load()())]

@patch('zgres._plugin.iter_entry_points')
def test_plugin_configure(iter_entry_points):
    plugin1 = Mock()
    plugin1.name = 'plugin1'
    iter_entry_points.return_value = iter([plugin1])
    config = ConfigParser()
    config.read_dict({
        'global': {
            'whatever': 'plugin1',
            },
        'plugin1': {
            'argA': 'A',
            'argB': 'B',
            }})
    plugins = get_configured_plugins(config, 'global', 'whatever')
    plugin1.load.assert_called_once_with(require=False)
    plugin1.load().assert_called_once_with(arga='A', argb='B')

def test_call_plugin():
    plugin1 = Mock()
    plugin2 = Mock()
    plugin2.side_effect = Exception('boom')
    plugin3 = Mock()
    call_plugins([
        ('p1', plugin1),
        ('p2', plugin2),
        ('p3', plugin3),
        ], 'arg1')
    plugin1.assert_called_once_with('arg1')
    plugin2.assert_called_once_with('arg1')
    plugin3.assert_called_once_with('arg1')
    # we never catch KeyboardInterrupt. so use that to test that we did indeed raise an exception
    plugin1.reset_mock()
    plugin2.reset_mock()
    plugin3.reset_mock()
    plugin2.side_effect = KeyboardInterrupt()
    with pytest.raises(KeyboardInterrupt) as exec:
        call_plugins([
            ('p1', plugin1),
            ('p2', plugin2),
            ('p3', plugin3),
            ], 'arg1')
    plugin1.assert_called_once_with('arg1')
    plugin2.assert_called_once_with('arg1')
    assert not plugin3.called
