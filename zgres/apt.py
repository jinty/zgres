import os
import time
import shutil
import asyncio
from asyncio import sleep
import logging
from subprocess import check_output, call, check_call

import psycopg2
import psycopg2.errorcodes

from . import systemd, utils
from .plugin import subscribe

def _pg_controldata_value(pg_version, data_dir, key):
    if not os.path.exists(os.path.join(data_dir, 'global', 'pg_control')):
        return None # cluster corrupt?
    data = check_output([
        '/usr/lib/postgresql/{}/bin/pg_controldata'.format(pg_version),
        data_dir])
    data = data.decode('latin-1') # I don't care, don't fail, the data I am interested in is ascii
    for line in data.splitlines():
        k, value = line.split(':', 1)
        if k == key:
            return value.strip()
    raise ValueError('could not find value for "{}"'.format(key))

class _NoCluster(Exception):
    pass

class AptPostgresqlPlugin:
    """Plugin for controlling postgresql installed by apt.

    NOTE: if you want to set the options in postgresql.conf, edit /etc/postgresql-common/createcluster.conf
    """

    def __init__(self, name, app):
        self.app = app
        self._systemd_check_key = '{}-systemd'.format(name)
        self._select1_check_key = '{}-select1'.format(name)
        self.logger = logging.getLogger(name)
        self._config_cache = {}
        self._config_mtime = None

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
        config_file = self._config_file()
        try:
            mtime = os.path.getmtime(config_file)
        except FileNotFoundError:
            raise _NoCluster()
        if mtime != self._config_mtime:
            # the file changed, invalidate our cache
            self._config_cache.clear()
            self._config_mtime = mtime
        value = self._config_cache.get(key, None)
        if value is None:
            value = check_output(['pg_conftool', '-s', self._version, self._cluster_name, 'show', key])
            value = value.decode('ascii').strip() # encoding unspecified, ascii is safe...
            if value.startswith("'") and value.endswith("'"):
                value = value[1:-1]
            self._config_cache[key] = value.replace("''", "'").replace("\\'", "'")
            return self._get_conf_value(key)
        return value

    def _port(self):
        return self._get_conf_value('port')

    def _socket_dir(self):
        return self._get_conf_value('unix_socket_directories').split(',')[0]

    def _data_dir(self):
        return self._get_conf_value('data_directory')
    
    def _default_data_dir(self):
        return '/var/lib/postgresql/{}/{}/'.format(self._version, self._cluster_name)

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
        changed = self._twiddle_config_file('pg_hba.conf') or changed
        changed = self._twiddle_config_file('pg_ident.conf') or changed
        return changed

    def _twiddle_config_file(self, config_file):
        prefix = config_file + '.'
        prefix_len = len(prefix)
        lines = {}
        for k, v in self.app.config['apt'].items():
            if k.startswith(prefix):
                k = k[prefix_len:]
                assert k not in lines
                lines[k] = v
        if not lines:
            return False
        mode = self.app.config['apt'].get(config_file, 'append')
        to_write = ['# {} by zgres'.format(mode)]
        for key, lines in sorted(lines.items()):
            to_write.append('')
            to_write.append('# added by zgres ({})'.format(key))
            to_write.append(lines)
        to_write.append('')
        path = self._config_file(name=config_file)
        if mode == 'append':
            with open(path, 'r') as f:
                currdata = f.read()
            to_write = [currdata, ''] + to_write
        elif mode == 'prepend':
            with open(path, 'r') as f:
                currdata = f.read()
            to_write = to_write + [currdata, '']
        elif mode == 'replace':
            pass
        else:
            raise ValueError(mode)
        with open(path, 'w') as f:
            currdata = f.write('\n'.join(to_write))
        return True

    @subscribe
    def pg_get_database_identifier(self):
        if not os.path.exists(self._config_file()):
            return None
        return _pg_controldata_value(self._version, self._data_dir(), 'Database system identifier')

    def _conn(self):
        info = self.pg_connect_info()
        return psycopg2.connect(**info)

    @subscribe
    def pg_get_timeline(self):
        if not os.path.exists(self._config_file()):
            return None
        if self._is_active() and not self._pg_is_in_recovery():
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

    def _pg_accepts_connections(self):
        retval = call(['sudo', '-u', 'postgres', 'psql', '-h', self._socket_dir(), '-p', self._port(), '-c', 'SELECT 1'])
        if retval:
            return False
        return True

    def _wait_for_connections(self):
        utils.backoff_wait(self._pg_accepts_connections, 0.1, times=300, message='Waiting for postgresql to accept connections')

    @subscribe
    def pg_start(self):
        # TODO: implement pg_hba.conf reconfig to allow cluster nodes to join the cluster automatically
        try:
            check_call(['systemctl', 'start', self._service()])
        except Exception:
            call(['systemctl', 'status', self._service()])
            raise
        self._wait_for_connections()
        if not self._is_active():
            raise Exception('Failed to start postgresql')

    @subscribe
    def pg_restart(self):
        check_call(['systemctl', 'restart', self._service()])

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
            if os.path.exists(self._config_file()):
                check_call(['rm', '-rf', self._data_dir(), self._config_dir])

    @subscribe
    def pg_initdb(self):
        if os.path.exists(self._config_file()):
            self.pg_stop()
            check_call(['pg_dropcluster', '--stop', self._version, self._cluster_name])
        if os.path.exists(self._default_data_dir()):
            check_call(['rm', '-rf', self._default_data_dir()])
        create_args = ['pg_createcluster', self._version, self._cluster_name]
        initdb_options = self.app.config['apt'].get('initdb_options', '').strip()
        if initdb_options:
            initdb_options = [i.strip() for i in initdb_options.split() if i.strip()]
            create_args.append('--')
            create_args.extend(initdb_options)
        check_call(['pg_createcluster', self._version, self._cluster_name, '--', '--data-checksums'])
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
    def pg_replication_role(self):
        try:
            return os.path.exists(os.path.join(self._data_dir(), 'recovery.conf')) and 'replica' or 'master'
        except _NoCluster:
            return None

    @subscribe
    def start_monitoring(self):
        loop = asyncio.get_event_loop()
        self.app.unhealthy(self._systemd_check_key, 'Waiting for first systemd check')
        loop.call_soon(loop.create_task, self._monitor_systemd())
        self.app.unhealthy(self._select1_check_key, 'Waiting for first select 1 check')
        loop.call_soon(loop.create_task, self._monitor_select1())
        loop.call_soon(loop.create_task, self._monitor_replication_role())

    def _is_active(self):
        return 0 == call(['systemctl', '--quiet', 'is-active', self._service()])

    async def _monitor_systemd(self):
        loop = asyncio.get_event_loop()
        while True:
            if self._is_active():
                self.app.healthy(self._systemd_check_key)
            else:
                await sleep(10)
                if not self._is_active():
                    self.app.unhealthy(self._systemd_check_key, 'inactive according to systemd')
            await sleep(5)

    def _can_select1(self):
        try:
            conn = self._conn()
            try:
                cur = conn.cursor()
                cur.execute('SELECT 1')
                cur.fetchall()
            finally:
                conn.close()
        except Exception:
            return False
        return True

    async def _monitor_select1(self):
        loop = asyncio.get_event_loop()
        while True:
            await sleep(1)
            if self._can_select1():
                self.app.healthy(self._select1_check_key)
            else:
                await sleep(2)
                if not self._can_select1():
                    self.app.unhealthy(self._select1_check_key, 'SELECT 1 failed')

    def _trigger_file(self):
        return '/var/run/postgresql/{}-{}.master_trigger'.format(self._version, self._cluster_name)

    def _pg_is_in_recovery(self):
        conn = self._conn()
        try:
            cur = conn.cursor()
            cur.execute('SELECT pg_is_in_recovery();')
            return cur.fetchall()[0][0]
        finally:
            conn.close()

    @subscribe
    def pg_stop_replication(self):
        assert self.pg_replication_role() == 'replica'
        trigger_file = self._trigger_file()
        with open(trigger_file, 'w') as f:
            f.write('touched')
        time.sleep(0.2)
        while True:
            # it might seem like a nice idea to timeout here, but it is NOT
            #
            # Postgres might be in recovery mode replaying WAL which can take
            # an ARBITRARY time. but it is on it's way to becoming a master.
            #
            # This can happen if we kill the whole cluster and just start
            # again from the archive
            try:
                if not self._pg_is_in_recovery():
                    break
            except psycopg2.OperationalError as e:
                pass
            logging.info('waiting for postgresql to come out of recovery')
            time.sleep(1)
        if self._set_config_values('master.'):
            self.pg_reload()

    @subscribe
    def pg_setup_replication(self, primary_conninfo):
        trigger_file = self._trigger_file()
        if os.path.exists(trigger_file):
            os.remove(trigger_file)
        config = """standby_mode = 'on'
trigger_file = '{trigger_file}'
recovery_target_timeline = 'latest'
"""
        config = config.format(trigger_file=trigger_file)
        restore_command = self.app.config['apt'].get('restore_command', None)
        if primary_conninfo:
            parts = []
            for k, v in primary_conninfo.items():
                parts.append('{}={}'.format(k, v))
            config += "\nprimary_conninfo = '{}'".format(' '.join(parts))
        if restore_command:
            config += "\nrestore_command = '{}'".format(restore_command)
        recovery_conf = os.path.join(self._data_dir(), 'recovery.conf')
        recovery_conf_new = recovery_conf + '.zgres_new'
        with open(recovery_conf_new, 'w') as f:
            f.write(config)
        shutil.chown(recovery_conf_new, user='postgres', group='postgres')
        os.rename(recovery_conf_new, recovery_conf)
        self._set_config_values('replica.')

    async def _monitor_replication_role(self):
        while True:
            await sleep(5)
            real_role = self.pg_replication_role()
            state_role = self.app.replication_role
            if real_role == 'master' and state_role == 'replica':
                self.logger.error('Did you promote postgres manually? zgres thinks its a replica but postgres is actually a master. Restarting to try get the master lock')
                self.app.restart(0)
            if real_role == 'master' and not self.app.have_master_lock:
                self.logger.error('Postgresql is a master, but we DONT have the lock. This should never happen. oh well, restarting to try make the best of it')
                self.app.restart(0)
