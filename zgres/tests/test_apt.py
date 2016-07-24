import os
import time
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

def print_log(cluster):
    # for debugging
    with open('/var/log/postgresql/postgresql-{}-{}.log'.format(*cluster), 'r') as f:
        print('PostgreSQL log for test:')
        print(f.read())

@pytest.fixture
def plugin(cluster):
    pg_version, cluster_name = cluster
    app = mock.Mock()
    app.config = dict(
            apt={
                'postgresql.conf.hot_standby': 'on',
                'postgresql.conf.wal_level': 'hot_standby',
                'postgresql_version': pg_version,
                'pg_hba.conf.allowroot': 'local all postgres peer map=allowroot',
                'pg_hba.conf': 'prepend',
                'pg_ident.conf.allowroot': 'allowroot root postgres\nallowroot postgres postgres',
                'superuser_connect_as': 'postgres',
                'postgresql_cluster_name': cluster_name})
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

async def _disabled_monitor():
    return

@pytest.mark.asyncio
async def test_monitoring(plugin, cluster):
    with mock.patch('zgres.apt.sleep') as sleep, mock.patch('zgres.apt.call') as subprocess_call, mock.patch('zgres.apt.AptPostgresqlPlugin._monitor_select1') as ignored:
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
        plugin._monitor_replication_role = _disabled_monitor
        plugin.start_monitoring()
        await sleeper.wait()
        assert plugin.app.mock_calls == [
                mock.call.unhealthy('zgres#apt-systemd', 'Waiting for first systemd check'),
                mock.call.unhealthy('zgres#apt-select1', 'Waiting for first select 1 check'),
                mock.call.healthy('zgres#apt-systemd'),
                mock.call.healthy('zgres#apt-systemd'),
                mock.call.unhealthy('zgres#apt-systemd', 'inactive according to systemd'),
                mock.call.healthy('zgres#apt-systemd'),
                ]
        subprocess_call.assert_has_calls(
                [mock.call(['systemctl', '--quiet', 'is-active', 'postgresql@{}-{}.service'.format(*cluster)]),
                    ] * len(retvals))

@needs_root
def test_idempotent(plugin, cluster):
    plugin.pg_initdb()
    plugin.pg_initdb()
    plugin.pg_start()
    plugin.pg_start()
    plugin.pg_reload()
    plugin.pg_reload()
    plugin.pg_stop()
    plugin.pg_stop()
    check_call(['pg_dropcluster'] + list(cluster))

@needs_root
def test_init_start_stop_drop(plugin, cluster):
    plugin.app.config['apt']['superuser_connect_as'] = 'root'
    plugin.app.config['apt']['create_superuser'] = 'true'
    plugin.pg_initdb()
    assert not plugin._is_active()
    plugin.pg_start()
    assert plugin._is_active()
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
def test_database_identifier(running_plugin):
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
def test_timeline(plugin, cluster):
    # new db allways start in timeline 1
    assert plugin.pg_get_timeline() == None
    plugin.pg_initdb()
    # tst as master
    assert plugin.pg_get_timeline() == 1
    plugin.pg_start()
    assert plugin.pg_get_timeline() == 1
    plugin.pg_stop()
    # now test again as slave
    plugin.pg_setup_replication(None)
    assert plugin.pg_get_timeline() == 1
    plugin.pg_start()
    assert plugin.pg_get_timeline() == 1
    plugin.pg_stop_replication()
    assert plugin.pg_get_timeline() == 2
    plugin.pg_stop()
    assert plugin.pg_get_timeline() == 2
    plugin.pg_reset()

@needs_root
def test_database_identifier_with_no_cluster_setup(plugin):
    plugin.pg_reset()
    assert plugin.pg_get_database_identifier() == None

@needs_root
def test_pg_hba(plugin, cluster):
    del plugin.app.config['apt']['pg_hba.conf.allowroot']
    del plugin.app.config['apt']['pg_hba.conf']
    plugin.app.config['apt']['pg_hba.conf.key1'] = 'host replication postgres otherhost trust'
    plugin.app.config['apt']['pg_hba.conf.key2'] = 'host replication postgres otherhost2 trust\nhost replication postgres otherhost3 trust'
    plugin.pg_initdb()
    hba_file = plugin._config_file(name='pg_hba.conf')
    with open(hba_file, 'r') as f:
        data = f.read()
    appended = '\n'.join(['',
                          '# added by zgres (key1)',
                          'host replication postgres otherhost trust',
                          '',
                          '# added by zgres (key2)',
                          'host replication postgres otherhost2 trust',
                          'host replication postgres otherhost3 trust',
                          ''])
    assert data.endswith(appended)
    check_call(['pg_dropcluster'] + list(cluster))

@needs_root
def test_pg_hba_replace(plugin, cluster):
    del plugin.app.config['apt']['pg_hba.conf.allowroot']
    plugin.app.config['apt']['pg_hba.conf'] = 'replace'
    plugin.app.config['apt']['pg_hba.conf.key1'] = 'host replication postgres otherhost trust'
    plugin.app.config['apt']['pg_hba.conf.key2'] = 'host replication postgres otherhost2 trust\nhost replication postgres otherhost3 trust'
    plugin.pg_initdb()
    hba_file = plugin._config_file(name='pg_hba.conf')
    with open(hba_file, 'r') as f:
        data = f.read()
    expected = '\n'.join(['# replace by zgres',
                          '',
                          '# added by zgres (key1)',
                          'host replication postgres otherhost trust',
                          '',
                          '# added by zgres (key2)',
                          'host replication postgres otherhost2 trust',
                          'host replication postgres otherhost3 trust',
                          ''])
    assert data == expected
    check_call(['pg_dropcluster'] + list(cluster))

@needs_root
def test_pg_hba_prepend(plugin, cluster):
    del plugin.app.config['apt']['pg_hba.conf.allowroot']
    plugin.app.config['apt']['pg_hba.conf'] = 'prepend'
    plugin.app.config['apt']['pg_hba.conf.key1'] = 'host replication postgres otherhost trust'
    plugin.app.config['apt']['pg_hba.conf.key2'] = 'host replication postgres otherhost2 trust\nhost replication postgres otherhost3 trust'
    plugin.pg_initdb()
    hba_file = plugin._config_file(name='pg_hba.conf')
    with open(hba_file, 'r') as f:
        data = f.read()
    expected = '\n'.join(['# prepend by zgres',
                          '',
                          '# added by zgres (key1)',
                          'host replication postgres otherhost trust',
                          '',
                          '# added by zgres (key2)',
                          'host replication postgres otherhost2 trust',
                          'host replication postgres otherhost3 trust',
                          ''])
    assert data.startswith(expected)
    check_call(['pg_dropcluster'] + list(cluster))

@needs_root
def test_pg_ident(plugin, cluster):
    del plugin.app.config['apt']['pg_ident.conf.allowroot']
    plugin.app.config['apt']['pg_ident.conf.key1'] = 'pg root postgres'
    plugin.app.config['apt']['pg_ident.conf.key2'] = 'pg admin postgres\npg staff postgres'
    plugin.pg_initdb()
    ident_file = plugin._config_file(name='pg_ident.conf')
    with open(ident_file, 'r') as f:
        data = f.read()
    appended = '\n'.join(['',
                          '# added by zgres (key1)',
                          'pg root postgres',
                          '',
                          '# added by zgres (key2)',
                          'pg admin postgres',
                          'pg staff postgres',
                          ''])
    assert data.endswith(appended)
    check_call(['pg_dropcluster'] + list(cluster))

@needs_root
def test_setup_replication_restore_command(plugin, cluster):
    plugin.pg_initdb()
    plugin.pg_start()
    plugin.pg_stop()
    plugin.app.config['apt']['restore_command'] = 'false'
    plugin.pg_setup_replication(None)
    plugin.pg_start()

@needs_root
def test_setup_replication_with_conn_info(plugin, cluster):
    plugin.pg_initdb()
    plugin.pg_start()
    plugin.pg_stop()
    plugin.pg_setup_replication(dict(host='127.0.0.1', port=5555))
    plugin.pg_start()

@needs_root
def test_reset_idempotent(plugin, cluster):
    plugin.pg_initdb()
    plugin.pg_reset()
    plugin.pg_reset()

@needs_root
def test_replication_role(plugin, cluster):
    # if we have no cluster setup, return None
    assert plugin.pg_replication_role() == None
    # if we initdb, we get a master
    plugin.pg_initdb()
    assert plugin.pg_replication_role() == 'master'
    # even if the master is running
    plugin.pg_start()
    assert plugin.pg_replication_role() == 'master'
    plugin.pg_stop()
    # setup replication and we get a replica
    plugin.pg_setup_replication(None)
    assert plugin.pg_replication_role() == 'replica'
    plugin.pg_start()
    assert plugin.pg_replication_role() == 'replica'
    # stopping replication brings us back to master
    plugin.pg_stop_replication()
    assert plugin.pg_replication_role() == 'master'
    plugin.pg_reset()
    assert plugin.pg_replication_role() == None
