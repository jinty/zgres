"""Plugin Machinery"""
import logging
from pkg_resources import iter_entry_points

def get_configured_plugins(config, config_section, group):
    """Returns a list of configured plugins.

    Plugins are configured in a config file section by their Entry points
    group. A configuration can be provided for them in the config file in a
    section of the SAME name as the plugin.

    The plugin is called with the configuration during setup.

    Plugins are sorted by the order they are specified in the config file.
    """
    assert not config[config.default_section], 'No default section allowed, sorry!'
    configured_plugins = [c.strip() for c in config[config_section][group].split(',')]
    seen = set([])
    for i in configured_plugins:
        if i in seen:
            raise AssertionError('Duplicate plugin in config: [{}]{} = {}'.format(config_section, group, i))
        seen.add(i)
    plugins = {}
    available_plugins = []
    for i in iter_entry_points(group):
        available_plugins.append(i.name)
        if i.name in configured_plugins:
            if i.name in plugins:
                raise Exception('Duplicate plugins for name {}, one was: {}'.format(i.name, plugin_factory))
            plugin_factory = i.load(require=False) #EEK never auto install ANYTHING
            plugins[i.name] = plugin_factory
    not_seen = set(configured_plugins) - set(plugins)
    if not_seen:
        raise Exception('Plugins were configured in the config file, but I could NOT find them: {}\n'
                'Available plugins: {}'.format(not_seen, available_plugins))
    plugins = [(i, plugins[i]) for i in configured_plugins] # set order to what is specified in config file
    configured_plugins = []
    for name, plugin_factory in plugins:
        plugin_config = {}
        if name in config:
            plugin_config = config[name]
        plugin = plugin_factory(**plugin_config)
        configured_plugins.append((name, plugin))
    return configured_plugins

def call_plugins(plugins, *args):
    """Call a set of plugins with arguments"""
    for name, plugin in plugins:
        try:
            plugin(*args)
        except Exception:
            logging.exception('Calling plugin {} ({}) failed with args: {}'.format(name, plugin, args))
