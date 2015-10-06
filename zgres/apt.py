import os
import asyncio
from asyncio import sleep
import logging
from subprocess import check_output, call, check_call

from . import systemd

class AptPostgresqlPlugin:
    """Plugin for controlling postgresql installed by apt.

    NOTE: if you want to set the options in postgresql.conf, edit /etc/postgresql-common/createcluster.conf
    """

    def __init__(self, name, app):
        self.app = app
        self._health_check_key = (name, 'systemd')
        self.logger = logging.getLogger(name)

    @property
    def _version(self):
        return self.app.config['apt']['postgresql_version']

    @property
    def _cluster_name(self):
        return self.app.config['apt']['postgresql_cluster_name']

    def _config_file(self):
        return '/etc/postgresql/{}/{}/postgresql.conf'.format(self._version, self._cluster_name)

    def _data_dir(self):
        # TODO: we should look in the config file for this
        return '/var/lib/postgresql/{}/{}/'.format(self._version, self._cluster_name)

    def _service(self):
        return 'postgresql@{}-{}.service'.format(self._version, self._cluster_name)

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
