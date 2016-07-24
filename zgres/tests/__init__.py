import asyncio
from asyncio import sleep
from unittest import mock
from unittest.mock import Mock
from pluggy import HookimplMarker

import pytest

from .. import plugin

class FakeSleeper:

    def __init__(self, max_loops=20):
        self.log = []
        self.max_loops = max_loops
        self._finished = asyncio.Event()
        self._next = asyncio.Event()
        self.wait = self._finished.wait
        asyncio.get_event_loop().call_later(5, self._watchdog)

    def _watchdog(self):
        self.finish()
        raise Exception('Timed Out')

    def finish(self):
        self._finished.set()
        self._next.set()

    def _check_finished(self):
        if self._finished.is_set():
            raise AssertionError('Already finished')

    async def __call__(self, delay):
        self.log.append(delay)
        self._next.set()
        await sleep(0)
        if self.max_loops is not None and len(self.log) >= self.max_loops:
            self.finish()
            await sleep(1)
        self._check_finished()

    async def next(self):
        # wait till the next __call__
        self._check_finished()
        self._next.clear()
        await self._next.wait()

@pytest.fixture
def deadman_app():
    def factory(config):
        defaults = dict(
                deadman={},
                )
        defaults.update(config)
        from .. import deadman
        return deadman.App(defaults)
    return factory

def MockSyncPlugin(name, app):
    from zgres.plugin import get_plugin_manager
    from zgres import sync
    pm = get_plugin_manager([], sync)
    p = mock.Mock(spec=['state', 'conn_info', 'masters', 'databases'])
    class Proxy:
        pass
    proxy = Proxy()
    hooks = setup_proxy(proxy, pm, p)
    for name in hooks:
        # return None for all hooks by default
        getattr(p, name).return_value = None
    return proxy, p

def mock_plugin(plugin_manager, mock=None, return_none=True):
    """Register a proxy which acts as a plugin for a mock object.

    The proxy has a function signature exactly like the specification
    registered in the plugin manager and defers all calls to the mock object.
    """
    if mock is None:
        mock = Mock()
    class Proxy:
        pass
    proxy = Proxy()
    hooks = setup_proxy(proxy, plugin_manager, mock)
    if return_none:
        for name in hooks:
            # return None for all hooks by default
            getattr(mock, name).return_value = None
    plugin_manager.register(proxy)
    return mock

def setup_proxy(proxy, plugin_manager, mock):
    mark = HookimplMarker(plugin_manager.project_name)
    hooks = []
    for function_name in dir(plugin_manager.hook):
        # find the specifications of the hooks in the plugin manager
        hook = getattr(plugin_manager.hook, function_name, None)
        if hook is None:
            continue
        has_spec = getattr(hook, 'has_spec', None)
        if has_spec is None or not has_spec():
            continue
        if getattr(mock, function_name, None) is None:
            # we were passed a mock object which does not implement this hook
            # skip it
            continue
        hooks.append(function_name)
        # create a function with the right signature which delegates
        # to the mock object
        args = [a for a in hook.argnames if a != '__multicall__']
        code = 'def {func}({args}):\n    return mock.{func}({args})'.format(
                func=function_name,
                args=', '.join(args))
        ns = dict(mock=mock)
        exec(code, ns)
        func = ns[function_name]
        func = mark(func)
        setattr(proxy, function_name, func)
    return hooks
