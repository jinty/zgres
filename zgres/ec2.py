mport asyncio
import boto
import boto.utils

from subprocess import check_call

def _integer_wal_pos(pos):
    # see http://eulerto.blogspot.com.es/2011/11/understanding-wal-nomenclature.html
    logfile, offset = pos.split('/')
    return 0xFF000000 * int(logfile, 16) + int(offset, 16)

class Ec2SnapshotBackupPlugin:

    _backing_up = asyncio.Lock()

    def __init__(self, name, app):
        self.app = app

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
        delf._device_options = devices

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

    def postgresql_backup(self):
        conn = self._conn()
        # find the EC2 volumes we should be backing up from their device names
        instance_volumes = self._get_volumes_for_our_devices(conn)
        # actually do the backup
        backup_id = str(self._uuid())
        pg_conn = self.app.postgresql_connect()
        cur = pg_conn.cursor()
        cur.execute("select pg_start_backup(%s);", (backup_id, ))
        position = cur.fetchall()[0][0]
        try:
            for d in self._devices:
                v = instance_volumes[d]
                snapshot = conn.create_snapshot(v.id, "Zgres backup")
                snapshot.add_tags({
                    'zgres:db_id': database_identifier,
                    'zgres:wal_position': position,
                    'zgres:device': d})
        finally:
            pg_conn.cursor().execute("select pg_stop_backup(%s);", (backup_id, ))

    def _get_my_snapshots(self, conn):
        snapshots = {}
        for snapshot in conn.get_all_snapshots(
                filter={
                    'status': 'completed',
                    'tag:zgres:db_id': database_identifier}):
            wal_pos = snapshot.tags.get('zgres:wal_position')
            if wal_pos is None:
                continue
            l = snapshots.setdefault(wal_pos, [])
            l.append(snapshot)
        # now order and discard those which do not have all devices
        snapshots = sorted(snapshots.items(), key=lambda x: _integer_wal_pos(x[0]))
        result = []
        wanted_snaps = set(self._devices)
        for wal_pos, unorderd_snaps in snapshots:
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

    def postgresql_restore(self):
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
