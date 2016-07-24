"""Plugin Machinery"""
import logging

from pkg_resources import iter_entry_points
import pluggy

_missing = object()
_logger = logging.getLogger('zgres')

hookspec = pluggy.HookspecMarker('zgres')
subscribe = pluggy.HookimplMarker('zgres')

def load(config, section):
    """Gets the plugin factories from the config file.

    Plugins are sorted by the order they are specified in the config file.
    """
    plugin_names = [c.strip() for c in config[section].get('plugins', '').split() if c.strip()]
    seen = set([])
    for i in plugin_names:
        if i in seen:
            raise AssertionError('Duplicate plugin in config: [{}]{} = {}'.format(config_section, group, i))
        seen.add(i)
    plugins = {}
    available_plugins = []
    for i in iter_entry_points('zgres.' + section):
        absolute_name = i.dist.project_name + '#' + i.name
        available_plugins.append(absolute_name)
        if absolute_name in plugin_names:
            if absolute_name in plugins:
                raise Exception('Duplicate plugins for name {}, one was: {}'.format(i.name, plugin_factory))
            plugin_factory = i.load(require=False) #EEK never auto install ANYTHING
            plugins[absolute_name] = plugin_factory
    not_seen = set(plugin_names) - set(plugins)
    if not_seen:
        raise Exception('Plugins were configured in the config file, but I could NOT find them: {}\n'
                'Available plugins: {}'.format(not_seen, available_plugins))
    return [(i, plugins[i]) for i in plugin_names] # set order to what is specified in config file

def configure(plugins, *args, **kw):
    """Setup all plugins by calling them with the passed arguments"""
    c_plugins = []
    for name, plugin_factory in plugins:
        plugin = plugin_factory(name, *args, **kw)
        c_plugins.append((name, plugin))
    return c_plugins

def _handlers_executor(handlers, event_name):
    if not handlers:
        return None
    def call(self, *args, **kw):
        result = [(name, h(*args, **kw)) for name, _, h in handlers]
        _logger.info('event {} called with {}, returning {}'.format(event_name, (args, kw), result))
        return result
    return call

def _handlers_executor_single(handlers, event_name):
    if not handlers:
        return None
    assert len(handlers) == 1
    handler = handlers[0][2]
    def call(self, *args, **kw):
        result = handler(*args, **kw)
        _logger.info('event {} called with {}, returning {}'.format(event_name, (args, kw), result))
        return result
    return call

def get_plugin_manager(setup_plugins, events, logger=_logger):
    logger.info('Loading Plugins')
    pm = pluggy.PluginManager('zgres')
    pm.add_hookspecs(events)
    setup_plugins = list(setup_plugins)
    setup_plugins.reverse()
    for name, plugin in setup_plugins:
        pm.register(plugin, name=name)
    return pm

def get_event_handler(setup_plugins, events, logger=_logger):
    return get_plugin_manager(setup_plugins, events, logger=_logger).hook

def get_plugins(config, section, hook_module, *plugin_config_args, **plugin_config_kw):
    plugins = load(config, section)
    plugins = configure(plugins, *plugin_config_args, **plugin_config_kw)
    return get_event_handler(plugins, hook_module)

def setup_plugins(config, section, hook_module, *plugin_config_args, **plugin_config_kw):
    plugins = load(config, section)
    plugins = configure(plugins, *plugin_config_args, **plugin_config_kw)
    return get_plugin_manager(plugins, hook_module)
