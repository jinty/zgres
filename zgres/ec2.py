import asyncio
import uuid

import boto
import boto.ec2
import boto.utils
import psycopg2

from subprocess import check_call

from .utils import pg_lsn_to_int
from .plugin import subscribe

class Ec2Plugin:

    def __init__(self, name, app):
        pass

    @subscribe
    def initialize(self):
        self._metadata = boto.utils.get_instance_metadata()

    @subscribe
    def get_my_id(self):
        return self._metadata['instance-id']

    @subscribe
    def get_conn_info(self):
        return {
                'host': self._metadata['local-ipv4'],
                'instance-type': self._metadata['instance-type'],
                'ami-id': self._metadata['ami-id'],
                'availability-zone': self._metadata['placement']['availability-zone'],
                }

class Ec2SnapshotBackupPlugin:

    _backing_up = asyncio.Lock()

    def __init__(self, name, app):
        self.app = app

    @subscribe
    def initialize(self):
        metadata = boto.utils.get_instance_metadata()
        self._instance_id = metadata['instance-id']
        self._availability_zone = metadata['placement']['availability-zone']
        self._region = self._availability_zone[:-1]
        devices = {}
        for k, v in self.app.config['ec2-snapshot'].items():
            if k.startswith('dev.'):
                _, device_no, key = k.strip().split('.', 2)
            device_no = int(device_no)
            devices.setdefault(device_no, {})[key] = v.strip()
        devices = [d[1] for d in sorted(devices.items())]
        self._devices = [d['device'] for d in devices]
        self._device_options = devices

    def _conn(self):
        return boto.ec2.connect_to_region(self._region)

    def _get_volumes_for_our_devices(self, conn):
        instance_volumes = {}
        for v in conn.get_all_volumes(filters={'attachment.instance-id': self._instance_id}):
            assert v.attach_data.instance_id == self._instance_id
            instance_volumes[v.attach_data.device] = v
        for d in self._devices:
            if d not in instance_volumes:
                raise Exception('Device {} in my config is not one that is attached to this instance. I have: {}'.format(
                    d, instance_volumes))
        return instance_volumes

    def _uuid(self):
        return uuid.uuid1()

    @subscribe
    def pg_backup(self):
        conn = self._conn()
        # find the EC2 volumes we should be backing up from their device names
        instance_volumes = self._get_volumes_for_our_devices(conn)
        # actually do the backup
        backup_id = str(self._uuid())
        pg_conn = psycopg2.connect(**self.app.pg_connect_info())
        cur = pg_conn.cursor()
        cur.execute("select pg_start_backup(%s);", (backup_id, ))
        position = cur.fetchall()[0][0]
        try:
            for d in self._devices:
                v = instance_volumes[d]
                snapshot = conn.create_snapshot(v.id, "Zgres backup")
                snapshot.add_tags({
                    'zgres:id': backup_id,
                    'zgres:db_id': self.app.database_identifier,
                    'zgres:wal_position': position,
                    'zgres:device': d})
        finally:
            pg_conn.cursor().execute("select pg_stop_backup();")

    def _get_my_snapshots(self, conn):
        snapshots = {}
        for snapshot in conn.get_all_snapshots(
                filters={
                    'status': 'completed',
                    'tag:zgres:db_id': self.app.database_identifier}):
            wal_pos = snapshot.tags.get('zgres:wal_position')
            if wal_pos is None:
                continue
            l = snapshots.setdefault(wal_pos, [])
            l.append(snapshot)
        # now order and discard those which do not have all devices
        snapshots = sorted(snapshots.items(), key=lambda x: pg_lsn_to_int(x[0]))
        result = []
        wanted_snaps = set(self._devices)
        for wal_pos, unordered_snaps in snapshots:
            got_devices = dict([(item.tags.get('zgres:device'), item) for item in unordered_snaps])
            if set(got_devices) != wanted_snaps:
                # different number of devices, we're ignoring this
                # incomplete? different config?
                continue
            result.append(dict(
                snapshots=got_devices,
                wal_position=wal_pos))
        return result

    def _detach_my_devices(self):
        instance_volumes = self._get_volumes_for_our_devices(conn)
        to_detach = list(self.devices)
        to_detach.reverse()
        for d in to_detach:
            vol = instance_volumes[d]
            check_call(['umount', self._ec2_device_to_local(d)])
            if not vol.detach():
                logging.error('Force detaching {}'.format(d))
                if not vol.detach(force=True):
                    raise Exception('Could not detach: {}'.format(d))
            vol.delete()

    def _ec2_device_to_local(self, device):
        """/dev/sdf -> /dev/xvdf"""
        return device.replace('/dev/sd', '/dev/xvd')

    @subscribe
    def pg_restore(self):
        conn = self._conn()
        snapshots = self._get_my_snapshots(conn)
        latest = snapshots[-1]
        snaps = latest['snapshots']
        to_attach = {}
        for d in self._device_options:
            snap = snaps[d['device']]
            vol = snap.create_volume(
                    self._availability_zone,
                    size=d.get('size'),
                    volume_type=d.get('volume_type'),
                    iops=d.get('iops'))
            to_attach[d['device']] = vol
        self._detach_my_devices()
        for d in self._device_options:
            to_attach[d['device']].attach(self._instance_id, d['device'])
            check_call(['mount', self._ec2_device_to_local(d['device'])])
