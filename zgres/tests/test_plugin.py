from unittest.mock import patch, Mock
from configparser import ConfigParser

import pytest

from .._plugin import *

class Plugin1:

    def __init__(self, name, log):
        self.log = log
        self.name = name

    def event(self, arg1):
        self.log.append((self.name, 'event', arg1))


class Plugin2:

    def __init__(self, name, log):
        self.log = log
        self.name = name

    def event(self, arg1):
        self.log.append((self.name, 'event', arg1))
        return arg1 + '-ho'

    def other_event(self, arg1, arg2):
        self.log.append((self.name, 'other_event', arg1, arg2))
        return arg1 + arg2

def test_real_plugins():
    config = ConfigParser()
    config.read_dict({
        'sync': {
            'plugins': 'zgres-apply',
            }})
    plugins = load(config, 'sync')
    from zgres.apply import Plugin
    assert plugins == [('zgres-apply', Plugin)]

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
        'whatever': dict(plugins='plugin1,plugin2'),
        'whatever_reverse': dict(plugins='plugin2,plugin1'),
        'other': dict(plugins='plugin2,plugin3'),
            })
    plugins = load(config, 'whatever')
    plugin1.load.assert_called_once_with(require=False)
    assert plugins == [('plugin1', plugin1.load()), ('plugin2', plugin2.load())]
    # reverse order in the config file and you see the plugins are returned in that order
    plugins = load(config, 'whatever_reverse')
    assert plugins == [('plugin2', plugin2.load()), ('plugin1', plugin1.load())]
    # try now with only one plugin configured
    assert not plugin3.load.called
    plugins = load(config, 'other')
    plugin3.load.assert_called_once_with(require=False)
    assert plugins == [('plugin2', plugin2.load()), ('plugin3', plugin3.load())]

def test_get_event_handler():
    log = []
    plugins = configure([
            ('plugin1', Plugin1),
            ('plugin2', Plugin2),
            ('plugin3', Plugin1),
            ], log)
    handler = get_event_handler(plugins, ['no_op_event', 'event', 'other_event'])
    assert log == []
    # The no-op event has no handlers, so it is defined as None
    assert handler.no_op_event is None
    # Call the real event
    result = handler.event('hey')
    assert result == [
        ('plugin1', None),
        ('plugin2', 'hey-ho'),
        ('plugin3', None),
        ]
    assert log == [
        ('plugin1', 'event', 'hey'),
        ('plugin2', 'event', 'hey'),
        ('plugin3', 'event', 'hey'),
        ]
    log[:] = []
    # with non-returning events, the events are just called
    result = handler.other_event(1, 5)
    assert result == [
        ('plugin2', 6),
        ]
    assert log == [
        ('plugin2', 'other_event', 1, 5),
        ]
