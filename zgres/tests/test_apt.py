from unittest import mock
import asyncio

import pytest
from . import FakeSleeper

@pytest.fixture
def plugin():
    app = mock.Mock()
    app.config = dict(
            apt=dict(
                postgresql_version=9.42,
                postgresql_cluster_name='answers'))
    from ..apt import AptPostgresqlPlugin
    return AptPostgresqlPlugin('zgres#apt', app)

def test_config_file(plugin):
    assert plugin._config_file(name='pg_hba.conf') == '/etc/postgresql/9.42/answers/pg_hba.conf'

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
        sleeper = FakeSleeper(max_loops=len(retvals) + 1)
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
