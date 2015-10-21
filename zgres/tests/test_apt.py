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
    return ('9.4', 'zgres_test')

@pytest.fixture
def plugin(cluster):
    pg_version, cluster_name = cluster
    app = mock.Mock()
    app.config = dict(
            apt=dict(
                postgresql_version=pg_version,
                postgresql_cluster_name=cluster_name,
                superuser_connect_as='root',
                create_superuser='True'))
    from ..apt import AptPostgresqlPlugin
    return AptPostgresqlPlugin('zgres#apt', app)

@pytest.fixture
def running_plugin(request, plugin, cluster):
    # shortcut to a running cluster
    plugin.pg_initdb()
    plugin.pg_start()
    def end():
        plugin.pg_stop()
        check_call(['pg_dropcluster'] + list(cluster))
    request.addfinalizer(end)
    return plugin

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
def test_double_initdb(plugin, cluster):
    plugin.pg_initdb()
    plugin.pg_initdb()
    check_call(['pg_dropcluster'] + list(cluster))

@needs_root
def test_init_start_stop_drop(plugin, cluster):
    plugin.pg_initdb()
    plugin.pg_start()
    conn_info = plugin.pg_connect_info()
    with psycopg2.connect(**conn_info) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT version(), current_setting('hba_file');")
            got_ver, hba_file = cur.fetchall()[0]
    assert got_ver.startswith('PostgreSQL {}'.format(cluster[0])), got_ver
    assert hba_file == plugin._config_file(name='pg_hba.conf')
    plugin.pg_stop()
    with pytest.raises(psycopg2.OperationalError):
        conn = psycopg2.connect(**conn_info)
    check_call(['pg_dropcluster'] + list(cluster))

@needs_root
def test_database_idntifier(running_plugin):
    # works when db is running
    ident = running_plugin.pg_get_database_identifier()
    assert int(ident) > 0
    # and when it is not
    running_plugin.pg_stop()
    assert ident == running_plugin.pg_get_database_identifier()
    # re-initing gives us a different number
    running_plugin.pg_initdb()
    new_ident = running_plugin.pg_get_database_identifier()
    assert int(new_ident) > 0
    assert ident != new_ident

@needs_root
def test_database_identifier_with_no_cluster_setup(plugin):
    # NOTE: this test may fail if others do not cleanup properly
    assert plugin.pg_get_database_identifier() == None
