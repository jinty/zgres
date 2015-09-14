from unittest.mock import patch, Mock
from configparser import ConfigParser

import pytest

from .._plugin import *

def mock_iter_entry_points(*names):
    for i in names:
        m = Mock()
        m.name = i
        yield m

def test_real_plugins():
    config = ConfigParser()
    config.read_dict({
        'global': {
            'zgres.conn': 'zgres-apply',
            }})
    plugins = get_configured_plugins(config, 'zgres.conn')
    from zgres.apply import conn_info_plugin
    assert plugins == [('zgres-apply', conn_info_plugin)]

@patch('zgres._plugin.iter_entry_points')
def test_multiple_plugins(iter_entry_points):
    plugin1 = Mock()
    plugin1.name = 'plugin1'
    plugin2 = Mock()
    plugin2.name = 'plugin2'
    iter_entry_points.return_value = iter([plugin2, plugin1])
    config = ConfigParser()
    config.read_dict({
        'global': {
            'whatever': 'plugin1,plugin2',
            }})
    plugins = get_configured_plugins(config, 'whatever')
    plugin1.load.assert_called_once_with(require=False)
    plugin1.load().assert_called_once_with()
    assert plugins == [('plugin1', plugin1.load()()), ('plugin2', plugin2.load()())]
    # try now with only one plugin configured
    plugin2.reset_mock()
    plugin1.reset_mock()
    iter_entry_points.return_value = iter([plugin2, plugin1])
    config = ConfigParser()
    config.read_dict({
        'global': {
            'whatever': 'plugin2',
            }})
    plugins = get_configured_plugins(config, 'whatever')
    plugin2.load.assert_called_once_with(require=False)
    plugin2.load().assert_called_once_with()
    assert plugins == [('plugin2', plugin2.load()())]
    assert not plugin1.load.called

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
    plugins = get_configured_plugins(config, 'whatever')
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
