import os
import asyncio
import logging
import uuid
import time

import boto
import boto.ec2
import boto.utils
import psycopg2

import shlex
from subprocess import call, check_call, check_output

from .utils import pg_lsn_to_int, backoff_wait
from .plugin import subscribe

logger = logging.getLogger('zgres')

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
    def condition():
        vol.update()
        status = vol.status
        return status == 'available'
    backoff_wait(
            condition,
            message='Waiting for volume to be available: {}'.format(vol.id),
            times=None,
            max_wait=60)

def _wait_for_volume_attached(vol):
    def condition():
        vol.update()
        attach_state = vol.attachment_state()
        return vol.status == 'in-use' and attach_state == 'attached'
    backoff_wait(
            condition,
            message='Waiting for volume to be attached: {}'.format(vol.id),
            times=None,
            max_wait=60)

class Ec2SnapshotBackupPlugin:

    _backing_up = asyncio.Lock()
    _current_master = None

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
            if not k.startswith('dev.'):
                continue
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
        usertags = {}
        for k, v in self.app.config['ec2-snapshot'].items():
            if k.startswith('tag.') and v.strip():
                usertags[k[4:]] = v.strip()
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
                tags = {
                    'zgres:id': backup_id,
                    'zgres:db_id': self.app.database_identifier,
                    'zgres:wal_position': position,
                    'zgres:device': d}
                tags.update(usertags)
                snapshot.add_tags(tags)
                def complete():
                    snapshot.update()
                    return snapshot.status in ('completed', 'error')
                backoff_wait(
                        complete,
                        message='Waiting for snapshot {} to complete'.format(snapshot.id),
                        times=None,
                        max_wait=120)
                if snapshot.status != 'completed':
                    raise Exception('Snapshot did not complete: {}'.format(snapshot.state))
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
            local_device = LocalDevice(d)
            if not local_device.exists():
                assert d not in instance_volumes, (d, instance_volumes)
                # was never there!
                continue
            vol = instance_volumes[d]
            logger.info('unmounting {}'.format(local_device))
            local_device.umount()
            logger.info('detaching {}'.format(vol.id))
            vol.detach()
            _wait_for_volume_available(vol)
            logger.info('deleting {}'.format(vol.id))
            vol.delete()

    @subscribe
    def pg_restore(self):
        conn = self._conn()
        snapshots = self._get_my_snapshots(conn)
        latest = snapshots[-1]
        snaps = latest['snapshots']
        to_attach = {}
        self._detach_my_devices(conn)
        logger.info('Creating volumes for {}'.format(list(self._device_options)))
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
        logger.info('Attaching volumes')
        # attach and wait for the volumes to be attached
        for d in self._device_options:
            to_attach[d['device']].attach(self._instance_id, d['device'])
        for vol in to_attach.values():
            _wait_for_volume_attached(vol)
        logger.info('Mounting everything')
        backoff_wait(
            _all_devices_mounted,
            message='Waiting to mount all drives in fstab',
            times=30)
        postmount = self.app.config['ec2-snapshot'].get('cmd_post_mount', '').splitlines()
        postmount = [i.strip() for i in postmount if i.strip()]
        for cmd in postmount:
            logger.info('Executing post-mount command: {}'.format(cmd))
            check_call(shlex.split(cmd))
        self._set_delete_on_termination(conn, 'replica')

    def _set_delete_on_termination(self, conn, role):
        # set delete on termination for all devices we just mounted
        # helps prevent costs from spiraling
        #
        # we default to false for master for safety
        default = {
                'replica': 'true',
                'master': 'false'}[role]
        values = []
        for d in self._device_options:
            setting = d.get('delete_on_{}_termination'.format(role), default)
            values.append('{}={}'.format(d['device'], setting))
        conn.modify_instance_attribute(
                self._instance_id,
                'BlockDeviceMapping',
                values)

    @subscribe
    def start_monitoring(self):
        backup_interval = self.app.config['ec2-snapshot'].get('backup_interval', '').strip()
        if backup_interval:
            backup_interval = int(backup_interval)
            loop = asyncio.get_event_loop()
            loop.call_soon(loop.create_task, self._scheduled_backup(backup_interval))

    @subscribe
    def master_lock_changed(self, owner):
        if self._current_master != self.app.my_id and owner == self.app.my_id:
            # if we become masetr, reset delete_on_termination
            try:
                conn = self._conn()
                self._set_delete_on_termination(conn, 'master')
            except:
                # but don't stop the takeover if the AWS API is down
                logger.exception('Failed to set delete_on_termination. Carrying on regardless')
        self._current_master = owner

    def _should_backup(self):
        # perform backup only if I am the current master
        return self._current_master == self.app.my_id

    async def _scheduled_backup(self, interval):
        loop = asyncio.get_event_loop()
        while True:
            await asyncio.sleep(interval)
            if self._should_backup():
                await loop.run_in_executor(None, self.pg_backup)

class LocalDevice:
    
    def __init__(self, ec2_device):
        self._local_device = self._ec2_device_to_local(ec2_device)

    def _ec2_device_to_local(self, device):
        """/dev/sdf -> /dev/xvdf"""
        return device.replace('/dev/sd', '/dev/xvd')

    def exists(self):
        return os.path.exists(self._local_device)

    def __str__(self):
        return self._local_device

    def umount(self):
        check_call(['umount', self._local_device])

def _all_devices_mounted():
    return not call(['mount', '--all'])
