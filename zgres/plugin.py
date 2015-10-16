"""Plugin Machinery"""
import logging
from pkg_resources import iter_entry_points

_missing = object()

def subscribe(func):
    """Mark a method as a zgres subscriber"""
    func._zgres_subscriber = True
    return func

def load(config, section):
    """Gets the plugin factories from the config file.

    Plugins are sorted by the order they are specified in the config file.
    """
    plugin_names = [c.strip() for c in config[section].get('plugins', '').split(',') if c.strip()]
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

def _handlers_executor(handlers):
    if not handlers:
        return None
    def call(self, *args, **kw):
        return [h(*args, **kw) for _, _, h in handlers]
    return call

def _handlers_executor_single(handlers):
    if not handlers:
        return None
    assert len(handlers) == 1
    handler = handlers[0][2]
    def call(self, *args, **kw):
        return handler(*args, **kw)
    return call

def get_event_handler(setup_plugins, events, logger=logging):
    logger.info('Loading Plugins')
    class Handler:
        plugins = dict(setup_plugins)
    event_names = set([])
    for event_name in events:
        if isinstance(event_name, dict):
            spec = event_name
            event_name = spec.pop('name')
        else:
            spec = dict(type='multiple',
                    required=False)
        assert not event_name.startswith('_')
        event_names.add(event_name)
        handlers = []
        for name, plugin in setup_plugins:
            handler = getattr(plugin, event_name, None)
            if handler is None:
                continue
            if not getattr(handler, '_zgres_subscriber', False):
                logger.debug('skiping method {} on plugin {} as it is not marked as a subscriber with @subscribe'.format(event_name, plugin))
                continue
            handlers.append((name, event_name, handler))
        logger.info("loading event subscribers for {}: {}".format(event_name, ','.join([name for name, _, _ in handlers])))
        if spec['required'] and not handlers:
            raise AssertionError('At least one plugin must implement {}'.format(event_name))
        if spec['type'] == 'multiple':
            executor = _handlers_executor(handlers)
        elif spec['type'] == 'single':
            if len(handlers) > 1:
                raise AssertionError('Only one plugin can implement {}'.format(event_name))
            executor = _handlers_executor_single(handlers)
        else:
            raise NotImplementedError('unknown event spec type')
        setattr(Handler, event_name, executor)
    for name, plugin in setup_plugins:
        for attr in dir(plugin):
            if attr in event_names:
                continue
            if attr.startswith('_'):
                continue
            val = getattr(plugin, attr)
            subscriber = getattr(val, '_zgres_subscriber', _missing)
            if subscriber is not _missing:
                raise AssertionError('plugin {} has a subscriber I dont recognise: {}'.format(name, attr))
    return Handler()

def get_plugins(config, section, events, *plugin_config_args, **plugin_config_kw):
    plugins = load(config, section)
    plugins = configure(plugins, *plugin_config_args, **plugin_config_kw)
    return get_event_handler(plugins, events)
