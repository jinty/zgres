import os
from subprocess import check_output

from . import systemd

class AptPostgresqlPlugin:
    """Plugin for controlling postgresql installed by apt.

    NOTE: if you want to set the options in postgresql.conf, edit /etc/postgresql-common/createcluster.conf
    """

    def __init__(self, name, app):
        self.app = app
        self._version = self.app.config['sync']['apt']['postgresql_version']
        self._cluster_name = self.app.config['sync']['apt']['postgresql_cluster_name']

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
