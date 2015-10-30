import asyncio
from asyncio import sleep
from unittest import mock

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
        from ..deadman import App, _PLUGIN_API
        with mock.patch('zgres.plugin.get_plugins') as get_plugins:
            get_plugins.return_value = mock.Mock(spec_set=[s['name'] for s in _PLUGIN_API])
            app = App(defaults)
        plugins = app._plugins
        return app
    return factory

def MockSyncPlugin(name, app):
    p = mock.Mock(spec=['state', 'conn_info', 'masters'])
    p.return_value = None
    p.state = plugin.subscribe(p.state)
    p.conn_info = plugin.subscribe(p.conn_info)
    p.masters = plugin.subscribe(p.masters)
    return p
