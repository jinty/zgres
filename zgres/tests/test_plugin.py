from unittest.mock import patch, Mock
from configparser import ConfigParser

import pytest

from ..plugin import *

class Plugin1:

    def __init__(self, name, log):
        self.log = log
        self.name = name

    @subscribe
    def event(self, arg1):
        self.log.append((self.name, 'event', arg1))

    def other_event(self, arg1, arg2):
        Exception('This event is NOT subscribed, so should never be executed')

class Plugin2:

    def __init__(self, name, log):
        self.log = log
        self.name = name

    @subscribe
    def event(self, arg1):
        self.log.append((self.name, 'event', arg1))
        return arg1 + '-ho'

    @subscribe
    def other_event(self, arg1, arg2):
        self.log.append((self.name, 'other_event', arg1, arg2))
        return arg1 + arg2

def test_no_plugin_config():
    config = ConfigParser()
    config.read_dict({
        'sync': {
            }})
    plugins = load(config, 'sync')
    from zgres.apply import Plugin
    assert plugins == []

def test_real_plugins():
    config = ConfigParser()
    config.read_dict({
        'sync': {
            'plugins': 'zgres#zgres-apply',
            }})
    plugins = load(config, 'sync')
    from zgres.apply import Plugin
    assert plugins == [('zgres#zgres-apply', Plugin)]

@patch('zgres.plugin.iter_entry_points')
def test_multiple_plugins(iter_entry_points):
    plugin1 = Mock()
    plugin1.name = 'plugin1'
    plugin1.dist.project_name = 'mymodule'
    plugin2 = Mock()
    plugin2.name = 'plugin2'
    plugin2.dist.project_name = 'mymodule'
    plugin3 = Mock()
    plugin3.name = 'plugin3'
    plugin3.dist.project_name = 'mymodule'
    iter_entry_points.return_value = [plugin2, plugin3, plugin1]
    config = ConfigParser()
    config.read_dict({
        'whatever': dict(plugins='mymodule#plugin1,mymodule#plugin2'),
        'whatever_reverse': dict(plugins='mymodule#plugin2,mymodule#plugin1'),
        'other': dict(plugins='mymodule#plugin2,mymodule#plugin3'),
            })
    plugins = load(config, 'whatever')
    plugin1.load.assert_called_once_with(require=False)
    assert plugins == [('mymodule#plugin1', plugin1.load()), ('mymodule#plugin2', plugin2.load())]
    # reverse order in the config file and you see the plugins are returned in that order
    plugins = load(config, 'whatever_reverse')
    assert plugins == [('mymodule#plugin2', plugin2.load()), ('mymodule#plugin1', plugin1.load())]
    # try now with only one plugin configured
    assert not plugin3.load.called
    plugins = load(config, 'other')
    plugin3.load.assert_called_once_with(require=False)
    assert plugins == [('mymodule#plugin2', plugin2.load()), ('mymodule#plugin3', plugin3.load())]

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
        None,
        'hey-ho',
        None,
        ]
    assert log == [
        ('plugin1', 'event', 'hey'),
        ('plugin2', 'event', 'hey'),
        ('plugin3', 'event', 'hey'),
        ]
    log[:] = []
    # with non-returning events, the events are just called
    result = handler.other_event(1, 5)
    assert result == [6]
    assert log == [
        ('plugin2', 'other_event', 1, 5),
        ]

def test_error_where_plugin_mis_names_event():
    log = []
    plugins = configure([
            ('plugin1', Plugin1),
            ], log)
    get_event_handler(plugins, ['event'])
    with pytest.raises(AssertionError) as exec:
        get_event_handler(plugins, ['evont_wuth_typo'])


def test_get_event_handler_with_single_event():
    # Test an "single" event. This is an event which can not have more than one handler
    log = []
    plugins = configure([
            ('plugin2', Plugin2),
            ], log)
    handler = get_event_handler(plugins, [dict(name='event', type='single', required=True), 'other_event'])
    # Call the real event
    result = handler.event('hey')
    assert result == 'hey-ho'
    assert log == [
        ('plugin2', 'event', 'hey'),
        ]
    # it is an error to configure no handlers for this event
    plugins = configure([
            ], log)
    with pytest.raises(AssertionError) as exec:
        handler = get_event_handler(plugins, [dict(name='event', type='single', required=True)])
    # unless not required
    plugins = configure([
            ], log)
    handler = get_event_handler(plugins, [dict(name='event', type='single', required=False)])
    assert handler.event is None
    # and more than one is allways an error
    plugins = configure([
            ('plugin1', Plugin1),
            ('plugin2', Plugin2),
            ], log)
    with pytest.raises(AssertionError) as exec:
        handler = get_event_handler(plugins, [dict(name='event', type='single', required=True)])
