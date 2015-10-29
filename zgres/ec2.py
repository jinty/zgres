import os
import asyncio
import logging
import uuid
import time

import boto
import boto.ec2
import boto.utils
import psycopg2

from subprocess import check_call, check_output

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

def _wait_for_volume_available(vol):
    time.sleep(5)
    while True:
        # we wait forever, otherwise we could get an EXPENSIVE runaway that creates MANY LARGE volumes
        vol.update()
        status = vol.status
        if status == 'available':
            break
        time.sleep(5)
        logging.warn('Waiting for volume to be available: {} ({})'.format(vol.id, status))

def _wait_for_volume_attached(vol):
    time.sleep(5)
    while True:
        # we wait forever, otherwise we could get an EXPENSIVE runaway that creates MANY LARGE volumes
        vol.update()
        attach_state = vol.attachment_state()
        if vol.status == 'in-use' and attach_state == 'attached':
            break
        time.sleep(5)
        logging.warn('Waiting for volume to be attach: {} ({})'.format(vol.id, vol.status, attach_state))

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
                count = 0
                while snap.status not in ('completed', 'error') and count < 720:
                    count += 1
                    time.sleep(10)
                    snap.update()
                    logging.info('Waiting for snapshot {} to complete'.format(snap.id))
                if snap.state != 'completed':
                    raise Exception('Snapshot did not complete: {}'.format(snap.state))
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

    def _detach_my_devices(self, conn):
        instance_volumes = self._get_volumes_for_our_devices(conn)
        to_detach = list(self._devices)
        to_detach.reverse()
        for d in to_detach:
            local_device = self._ec2_device_to_local(d)
            if not os.path.exists(local_device):
                assert d not in instance_volumes, (d, instance_volumes)
                # was never there!
                continue
            vol = instance_volumes[d]
            logging.info('unmounting {}'.format(local_device))
            check_call(['umount', local_device])
            mounts = check_output(['mount']).decode('latin-1')
            for i in mounts.splitlines():
                parts = i.split()
                if parts and parts[0] == local_device:
                    check_call(['fuser', '-m', '-u', 'local_device'])
                    raise Exception('Device did not unmount:\n{}'.format(mounts))
            logging.info('detaching {}'.format(vol.id))
            vol.detach()
            _wait_for_volume_available(vol)
            logging.info('deleting {}'.format(vol.id))
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
        self._detach_my_devices(conn)
        logging.info('Creating volumes for {}'.format(list(self._device_options)))
        for d in self._device_options:
            snap = snaps[d['device']]
            vol = snap.create_volume(
                    self._availability_zone,
                    size=d.get('size'),
                    volume_type=d.get('volume_type'),
                    iops=d.get('iops'))
            to_attach[d['device']] = vol
        # wait for all the volumes to be available
        for vol in to_attach.values():
            _wait_for_volume_available(vol)
        logging.info('Attaching volumes')
        # attach and wait for the volumes to be attached
        for d in self._device_options:
            to_attach[d['device']].attach(self._instance_id, d['device'])
        for vol in to_attach.values():
            _wait_for_volume_attached(vol)
        logging.info('Mounting everything')
        # finally, actually mount them all
        for i in range(60):
            # systemd may already have mounted it, let's be sure it worked before continuing
            try:
                check_call(['mount', '--all'])
            except Exception:
                time.sleep(5)
            break
        else:
            check_call(['mount', '--all'])
