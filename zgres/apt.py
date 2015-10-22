import os
import shutil
import asyncio
from asyncio import sleep
import logging
from subprocess import check_output, call, check_call

import psycopg2

from . import systemd
from .plugin import subscribe

def _pg_controldata_value(pg_version, data_dir, key):
    data = check_output([
        '/usr/lib/postgresql/{}/bin/pg_controldata'.format(pg_version),
        data_dir])
    data = data.decode('latin-1') # I don't care, don't fail, the data I am interested in is ascii
    for line in data.splitlines():
        k, value = line.split(':', 1)
        if k == key:
            return value.strip()
    raise ValueError('could not find value for "{}"'.format(key))

class AptPostgresqlPlugin:
    """Plugin for controlling postgresql installed by apt.

    NOTE: if you want to set the options in postgresql.conf, edit /etc/postgresql-common/createcluster.conf
    """

    def __init__(self, name, app):
        self.app = app
        self._health_check_key = (name, 'systemd')
        self.logger = logging.getLogger(name)

    @property
    def _create_superuser(self):
        must_create = self.app.config['apt'].get('create_superuser', '').lower().strip() in ('t', 'true')
        if must_create and self._superuser_connect_as is None:
            raise ValueError('must set superuser_connect_as if create_superuser is True')
        return must_create

    @property
    def _superuser_connect_as(self):
        val = self.app.config['apt'].get('superuser_connect_as')
        if val is not None:
            val = val.strip()
        return val

    @property
    def _version(self):
        return self.app.config['apt']['postgresql_version']

    @property
    def _cluster_name(self):
        return self.app.config['apt']['postgresql_cluster_name']

    @property
    def _config_dir(self):
        return self.app.config['apt'].get('config_dir')

    def _pg_config_dir(self):
        return '/etc/postgresql/{}/{}/'.format(self._version, self._cluster_name)

    def _config_file(self, name='postgresql.conf'):
        return os.path.join(self._pg_config_dir(), name)

    def _set_conf_value(self, key, value):
        value = check_call(['pg_conftool', self._version, self._cluster_name, 'set', key, value])

    def _get_conf_value(self, key):
        value = check_output(['pg_conftool', '-s', self._version, self._cluster_name, 'show', key])
        value = value.decode('ascii').strip() # encoding unspecified, ascii is safe...
        if value.startswith("'") and value.endswith("'"):
            value = value[1:-1]
        return value.replace("''", "'").replace("\\'", "'")

    def _port(self):
        return self._get_conf_value('port')

    def _socket_dir(self):
        return self._get_conf_value('unix_socket_directories').split(',')[0]

    def _data_dir(self):
        return self._get_conf_value('data_directory')

    def _service(self):
        return 'postgresql@{}-{}.service'.format(self._version, self._cluster_name)

    def _copy_config(self):
        if not self._config_dir:
            return
        for filename in ['environment', 'pg_ctl.conf', 'pg_hba.conf', 'pg_ident.conf', 'postgresql.conf', 'start.conf']:
            source = os.path.join(self._config_dir, filename)
            if not os.path.exists(source):
                # no source, so don't replace target
                continue
            destination = self._config_file(filename)
            shutil.copyfile(source, destination)

    def _set_config_values(self, prefix=None):
        changed = False
        pg_hba = {}
        pg_ident = {}
        for k, v in self.app.config['apt'].items():
            if prefix is not None:
                if k.startswith(prefix):
                    k = k[len(prefix):]
                else:
                    continue
            if k.startswith('postgresql.conf.'):
                k = k[16:]
                v = v.strip()
                changed = True
                self._set_conf_value(k, v)
            elif k.startswith('pg_hba.conf.'):
                k = k[12:]
                assert k not in pg_hba
                pg_hba[k] = v
            elif k.startswith('pg_ident.conf.'):
                k = k[14:]
                assert k not in pg_ident
                pg_ident[k] = v
        if pg_ident or pg_hba:
            changed = True
        self._append_lines('pg_hba.conf', pg_hba)
        self._append_lines('pg_ident.conf', pg_ident)
        return changed

    def _append_lines(self, config_file, lines):
        if not lines:
            return
        path = self._config_file(name=config_file)
        with open(path, 'a') as f:
            f.write('\n')
            for key, lines in sorted(lines.items()):
                f.write('# added by zgres ({})\n'.format(key))
                f.write(lines)
                f.write('\n')
    @subscribe
    def pg_get_database_identifier(self):
        if not os.path.exists(self._config_file()):
            return None
        return _pg_controldata_value(self._version, self._data_dir(), 'Database system identifier')

    def _conn(self):
        info = self.pg_conn_info()
        return psycopg2.connect(**kw)

    @subscribe
    def pg_get_timeline(self):
        if not os.path.exists(self._config_file()):
            return None
        if self._is_active():
            conn = self._conn()
            cur = conn.cursor()
            cur.execute('SELECT pg_xlogfile_name(pg_current_xlog_insert_location());')
            wal_filename = cur.fetchall()[0][0]
            timeline_hex = wal_filename[:8]
            return int(timeline_hex, 16)
        else:
            val = _pg_controldata_value(
                    self._version,
                    self._data_dir(),
                    "Latest checkpoint's TimeLineID")
            return int(val)
        return int(val)

    @subscribe
    def pg_start(self):
        # TODO: implement pg_hba.conf reconfig to allow cluster nodes to join the cluster automatically
        check_call(['systemctl', 'start', self._service()])
        if not self._is_active():
            raise Exception('Failed to start postgresql')

    @subscribe
    def pg_reload(self):
        check_call(['systemctl', 'reload', self._service()])

    @subscribe
    def pg_stop(self):
        check_call(['systemctl', 'stop', self._service()])

    @subscribe
    def pg_reset(self):
        self.pg_stop()
        try:
            check_call(['pg_dropcluster', '--stop', self._version, self._cluster_name])
        except:
            check_call(['rm', '-rf', self._data_dir(), self._config_dir])

    @subscribe
    def pg_initdb(self):
        if os.path.exists(self._config_file()):
            self.pg_stop()
            check_call(['pg_dropcluster', '--stop', self._version, self._cluster_name])
        check_call(['pg_createcluster', self._version, self._cluster_name])
        self._copy_config()
        self._set_config_values()
        if self._create_superuser:
            self.pg_start()
            check_call(['sudo', '-u', 'postgres', 'createuser', '-s', '-h', self._socket_dir(), '-p', self._port(), self._superuser_connect_as])
            self.pg_stop()

    @subscribe
    def pg_connect_info(self):
        info = dict(database='postgres', user=self._superuser_connect_as, host=self._socket_dir(), port=self._port())
        user = self._superuser_connect_as
        if user is not None:
            info['user'] = user
        return info

    @subscribe
    def get_conn_info(self):
        return dict(port=self._port())

    @subscribe
    def pg_am_i_replica(self):
        return os.path.exists(os.path.join(self._data_dir(), 'recovery.conf'))

    @subscribe
    def start_monitoring(self):
        self.app.unhealthy(self._health_check_key, 'Waiting for first systemd check')
        loop = asyncio.get_event_loop()
        loop.call_soon(loop.create_task, self._monitor_systemd())

    def _is_active(self):
        return 0 == call(['systemctl', 'is-active', self._service()])

    async def _monitor_systemd(self):
        loop = asyncio.get_event_loop()
        while True:
            await sleep(1)
            if self._is_active():
                self.app.healthy(self._health_check_key)
            else:
                await sleep(2)
                if not self._is_active():
                    self.app.unhealthy(self._health_check_key, 'inactive according to systemd')

    def _trigger_file(self):
        return '/var/run/postgresql/{}-{}.master_trigger'.format(self._version, self._cluster_name)

    @subscribe
    def pg_stop_replication(self):
        if self._set_config_values('master.'):
            self.pg_reload()
        trigger_file = self._trigger_file()
        with open(trigger_file, 'w') as f:
            f.write('touched')

    @subscribe
    def pg_setup_replication(self, primary_conninfo=None):
        trigger_file = self._trigger_file()
        if os.path.exists(trigger_file):
            os.remove(trigger_file)
        config = """standby_mode = 'on'
trigger_file = '{trigger_file}'
recovery_target_timeline = 'latest'
"""
        config = config.format(
                trigger_file=trigger_file,
                host=host,
                port=port)
        restore_command = self.app.config['apt'].get('restore_command', None)
        if primary_conninfo:
            parts = []
            for k, v in primary_conninfo.items():
                parts.append('{}={}'.format(k, v))
            config += "\nprimary_conninfo = '{}'".format(' '.join(parts))
        if restore_command:
            config += "\nrestore_command = '{restore_command}'".format(restore_command)
        with open(os.path.join(self._data_dir(), 'recovery.conf'), 'w') as f:
            f.write(config)
        self._set_config_values('replica.')
