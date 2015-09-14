def get_configured_plugins(config, group):
    """Returns a list of configured plugins.

    Plugins are configured in the config file "global" section by their Entry
    points group. A configuration can be provided for them in the config file
    in a section of the SAME name as the plugin.

    The plugin is called with the configuration during setup.

    Plugins are sorted by name.
    """
    configured_plugins = [c.strip() for c in config['global'][group].split(',')]
    seen = set([])
    plugins = []
    for i in iter_entry_points(group):
        if i.name in configured_plugins:
            plugin_factory = i.load(require=False) #EEK never auto install ANYTHING
            if i.name in seen:
                raise Exception('Duplicate plugins for name {}, one was: {}'.format(i.name, plugin_factory))
            plugins.append((name, plugin_factory))
    not_seen = set(configured_plugins) - seen
    if not_seen:
        raise Exception('Plugins were configured in the config file, but I could NOT find them: {}'.format(not_seen))
    plugins = sorted(plugins) # consistent order of calling plugins
    configured_plugins = []
    for name, plugin_factory in plugins:
        plugin_config = config.get(name, {})
        plugin = plugin_factory(**plugin_config)
        configured_plugins.append(plugin)
    return configured_plugins

def call_plugins(plugins, *args):
    """Call a set of plugins with arguments"""
    for name, plugin in plugins:
        try:
            plugin(*args)
        except:
            logging.error('Calling plugin {} ({}) failed with args: {}'.format(name, plugin, args))
            logging.exception()


