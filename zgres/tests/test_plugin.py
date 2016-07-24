import sys
from unittest.mock import patch, Mock
from configparser import ConfigParser

import pytest

from ..plugin import *

@hookspec
def event(arg1):
    pass

@hookspec
def other_event(arg1, arg2):
    pass

@hookspec
def no_op_event(arg1, arg2):
    pass

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
        'whatever': dict(plugins='mymodule#plugin1\nmymodule#plugin2'),
        'whatever_reverse': dict(plugins='mymodule#plugin2\nmymodule#plugin1'),
        'other': dict(plugins='mymodule#plugin2\nmymodule#plugin3'),
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
    handler = get_event_handler(plugins, sys.modules[__name__])
    assert log == []
    # The no-op event has no handlers, but it can still be called
    assert handler.no_op_event(arg1='a', arg2='b') == []
    # Call the real event
    result = handler.event(arg1='hey')
    assert result == ['hey-ho']
    print(log)
    assert log == [
        ('plugin1', 'event', 'hey'),
        ('plugin2', 'event', 'hey'),
        ('plugin3', 'event', 'hey'),
        ]
    log[:] = []
    # with non-returning events, the events are just called
    result = handler.other_event(arg1=1, arg2=5)
    assert result == [6]
    assert log == [
        ('plugin2', 'other_event', 1, 5),
        ]

def test_get_event_handler_with_single_event():
    # Test an "single" event. This is an event which can not have more than one handler
    log = []
    plugins = configure([
            ('plugin2', Plugin2),
            ], log)
    class Spec:

        @hookspec(firstresult=True)
        def event(self, arg1):
            pass

        @hookspec
        def other_event(self, arg1, arg2):
            pass
    handler = get_event_handler(plugins, Spec)
    # Call the real event
    result = handler.event(arg1='hey')
    assert result == 'hey-ho'
    assert log == [
        ('plugin2', 'event', 'hey'),
        ]
    # If more than one plugin is registed, the first non-null result is used
    class Plugin:

        def __init__(self, name, log):
            self.log = log
            self.name = name

        @subscribe
        def event(self, arg1):
            self.log.append((self.name, 'event', arg1))
            return arg1 + '-he'

    plugins = configure([
            ('pluginA', Plugin1),
            ('pluginB', Plugin),
            ('pluginC', Plugin2),
            ], log)
    handler = get_event_handler(plugins, Spec)
    result = handler.event(arg1='hey')
    assert result == 'hey-he'
