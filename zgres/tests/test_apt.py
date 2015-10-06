from unittest import mock
import asyncio

import pytest

@pytest.fixture
def plugin():
    app = mock.Mock()
    app.config = dict(
            apt=dict(
                postgresql_version=9.42,
                postgresql_cluster_name='answers'))
    from ..apt import AptPostgresqlPlugin
    return AptPostgresqlPlugin('zgres#apt', app)

class FakeSleeper:

    def __init__(self, loops=1):
        self.log = []
        self.loops = loops
        self.finished = asyncio.Event()
        self.wait = self.finished.wait
        asyncio.get_event_loop().call_later(1, self._watchdog)

    def _watchdog(self):
        self.finished.set()
        raise Exception('Timed Out')

    async def __call__(self, delay):
        self.log.append(delay)
        if len(self.log) >= self.loops:
            self.finished.set()
            await asyncio.sleep(10000)
            raise AssertionError('boom')

def test_config_file(plugin):
    assert plugin._data_dir() == '/var/lib/postgresql/9.42/answers/'

@pytest.mark.asyncio
async def test_monitoring(plugin):
    with mock.patch('zgres.apt.sleep') as sleep, mock.patch('zgres.apt.call') as subprocess_call:
        retvals = [
                0, # become healthy
                1, # noop
                0, 0, # become healthy
                6, 5, # become unhelathy after 2 failed checks
                0, # become healthy
                ]
        subprocess_call.side_effect = retvals
        sleeper = FakeSleeper(loops=len(retvals) + 1)
        sleep.side_effect = sleeper
        plugin.start_monitoring()
        await sleeper.wait()
        assert plugin.app.mock_calls == [
                mock.call.unhealthy(('zgres#apt', 'systemd'), 'Waiting for first systemd check'),
                mock.call.healthy(('zgres#apt', 'systemd')),
                mock.call.healthy(('zgres#apt', 'systemd')),
                mock.call.unhealthy(('zgres#apt', 'systemd'), 'inactive according to systemd'),
                mock.call.healthy(('zgres#apt', 'systemd')),
                ]
        subprocess_call.assert_has_calls(
                [mock.call(['systemctl', 'is-active', 'postgresql@9.42-answers.service'])] * len(retvals))
