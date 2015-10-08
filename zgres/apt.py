import os
import asyncio
from asyncio import sleep
import logging
from subprocess import check_output, call, check_call

from . import systemd
import psycopg2

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
        return self.app.config['apt'].get('create_superuser', '').lower().strip() in ('t', 'true')

    @property
    def _superuser_connect_as(self):
        return self.app.config['apt']['superuser_connect_as'].strip()

    @property
    def _version(self):
        return self.app.config['apt']['postgresql_version']

    @property
    def _cluster_name(self):
        return self.app.config['apt']['postgresql_cluster_name']

    @property
    def _config_dir(self):
        return self.app.config['apt']['config_dir']

    def _pg_config_dir(self):
        return '/etc/postgresql/{}/{}/'.format(self._version, self._cluster_name)

    def _config_file(self, name='postgresql.conf'):
        return os.path.join(self._pg_config_dir(), name)

    def _get_conf_value(self, key):
        value = check_output(['pg_conftool', '-s', '9.4', 'main', 'show', key])
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

    def _assert_config(self):
        for filename in ['environment', 'pg_ctl.conf', 'pg_hba.conf', 'pg_ident.conf', 'postgresql.conf', 'start.conf']:
            source = os.path.join(self._config_dir, filename)
            if not os.path.exists(source):
                # no source, so don't replace target
                continue
            destination = self._config_file(filename)
            if os.path.realpath(source) == os.path.realpath(destination) and os.path.islink(destination):
                # dest is a symbolic link and is the same as source, nothing to do
                continue
            # ok, so replace
            if os.path.exists(destination):
                os.remove(destination)
            os.symlink(source, destination)

    def postgresql_get_database_identifier(self):
        if not os.path.exists(self._config_file()):
            return None
        data = check_output([
            '/usr/lib/postgresql/{}/bin/pg_controldata'.format(self._version),
            self._data_dir()])
        data = data.decode('latin-1') # I don't care, don't fail, the data I am interested in is ascii
        for line in data.splitlines():
            if line.startswith('Data page checksum version:'):
                _, dbid = line.split(':', 1)
                dbid = dbid.strip()
                return dbid
        return None

    def postgresql_start(self):
        check_call(['systemctl', 'start', self._service()])

    def postgresql_stop(self):
        check_call(['systemctl', 'stop', self._service()])

    def postgresql_initdb(self):
        if os.path.exists(self._config_file()):
            check_call(['pg_dropcluster', '--stop', self._version, self._cluster_name])
        check_call(['pg_createcluster', self._version, self._cluster_name])
        self._assert_config()
        if self._superuser_connect_as:
            check_call(['createuser', '-s', '-h', self._socket_dir, '-p', self._port, self._superuser_connect_as])

    def postgresql_connect(self):
        return psycopg2.connect(database='postgres', user=self._superuser_connect_as, host=self._socket_dir)

    def postgresql_am_i_replica(self):
        return os.path.exists(os.path.join(self._data_dir(), 'recovery.conf'))

    def start_monitoring(self):
        self.app.unhealthy(self._health_check_key, 'Waiting for first systemd check')
        loop = asyncio.get_event_loop()
        loop.call_soon(loop.create_task, self._monitor_systemd())

    async def _monitor_systemd(self):
        loop = asyncio.get_event_loop()
        while True:
            await sleep(1)
            status = call(['systemctl', 'is-active', self._service()])
            if status == 0:
                self.app.healthy(self._health_check_key)
            else:
                await sleep(2)
                status = call(['systemctl', 'is-active', self._service()])
                if status != 0:
                    self.app.unhealthy(self._health_check_key, 'inactive according to systemd')
