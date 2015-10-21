import os
from unittest import mock
import asyncio
from subprocess import check_output, check_call

import pytest
import psycopg2
from . import FakeSleeper

def have_root():
    destroy = os.environ.get('ZGRES_DESTROY_MACHINE', 'false').lower()
    if destroy in ['t', 'true']:
        user = check_output(['whoami']).decode('latin1').strip()
        if user != 'root':
            raise Exception('I need to run as root if you want me to destroy the machine! I am {}'.format(repr(user)))
        return True
    return False

needs_root = pytest.mark.skipif(not have_root(), reason='requires root and ZGRES_DESTROY_MACHINE=true in the environment')

@pytest.fixture
def cluster():
    return ('9.4', 'zgres-test')

@pytest.fixture
def plugin(cluster):
    pg_version, cluster_name = cluster
    app = mock.Mock()
    app.config = dict(
            apt=dict(
                postgresql_version=pg_version,
                postgresql_cluster_name=cluster_name))
    from ..apt import AptPostgresqlPlugin
    return AptPostgresqlPlugin('zgres#apt', app)

def test_config_file(plugin, cluster):
    assert plugin._config_file(name='pg_hba.conf') == '/etc/postgresql/{}/{}/pg_hba.conf'.format(*cluster)

@pytest.mark.asyncio
async def test_monitoring(plugin, cluster):
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
                [mock.call(['systemctl', 'is-active', 'postgresql@{}-{}.service'.format(*cluster)]),
                    ] * len(retvals))

@needs_root
def test_travis(plugin, cluster):
    plugin.pg_initdb()
    plugin.pg_start()
    conn_info = plugin.pg_connect_info()
    with psycopg2.connect(**conn_info) as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT version(), current_database();')
            got_ver, got_db = cur.fetchall()[0]
    assert got_ver == cluster[0]
    assert got_db == 'PostgreSQL {}'.format(cluster[1])
    check_call(['pg_dropcluster'] + list(cluster))
