"""Plugin Machinery"""
import logging
from pkg_resources import iter_entry_points

def load(config, section):
    """Gets the plugin factories from the config file.

    Plugins are sorted by the order they are specified in the config file.
    """
    assert not config[config.default_section], 'No default section allowed, sorry!'
    plugin_names = [c.strip() for c in config[section]['plugins'].split(',')]
    seen = set([])
    for i in plugin_names:
        if i in seen:
            raise AssertionError('Duplicate plugin in config: [{}]{} = {}'.format(config_section, group, i))
        seen.add(i)
    plugins = {}
    available_plugins = []
    for i in iter_entry_points('zgres.' + section):
        available_plugins.append(i.name)
        if i.name in plugin_names:
            if i.name in plugins:
                raise Exception('Duplicate plugins for name {}, one was: {}'.format(i.name, plugin_factory))
            plugin_factory = i.load(require=False) #EEK never auto install ANYTHING
            plugins[i.name] = plugin_factory
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

def _handlers_executor(handlers):
    if not handlers:
        return None
    def call(self, *args, **kw):
        return [(plugin_name, h(*args, **kw)) for plugin_name, _, h in handlers]
    return call

def get_event_handler(setup_plugins, events):
    class Handler:
        pass
    for event_name in events:
        handlers = []
        for name, plugin in setup_plugins:
            handler = getattr(plugin, event_name, None)
            if handler is None:
                continue
            handlers.append((name, event_name, handler))
        executor = _handlers_executor(handlers)
        setattr(Handler, event_name, executor)
    return Handler()

def call_plugins(plugins, *args):
    """Call a set of plugins with arguments"""
    for name, plugin in plugins:
        try:
            plugin(*args)
        except Exception:
            logging.exception('Calling plugin {} ({}) failed with args: {}'.format(name, plugin, args))
