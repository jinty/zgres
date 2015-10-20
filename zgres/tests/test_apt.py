import os
from unittest import mock
import asyncio
from subprocess import check_output

import pytest
from . import FakeSleeper

def have_root():
    print(dict(os.environ))
    destroy = os.environ.get('ZGRES_DESTROY_MACHINE', 'false').lower()
    if destroy in ['t', 'true']: 
        user = check_output(['whoami']).decode('latin1').strip()
        if user != 'root':
            raise Exception('I need to run as root if you want me to destroy the machine! I am {}'.format(repr(user)))
        return True
    return False

needs_root = pytest.mark.skipif(not have_root(), reason='requires root and ZGRES_DESTROY_MACHINE=true in the environment')

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

@needs_root
def test_travis():
    from subprocess import check_call
    check_call(['ls', '/etc/postgresql'])
    assert False, 'boom'
