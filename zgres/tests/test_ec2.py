import pytest
from unittest import mock

from moto import mock_ec2, mock_iam


def mock_get_instance_metadata(**kw):
    metadata = {
                'ami-id': 'ami-87d579ec',
                'instance-id': 'i-00000001',
                'instance-type': 't2.micro',
                'local-ipv4': '10.1.0.107',
                'placement': {'availability-zone': 'us-east-1d'},
                'public-ipv4': '52.5.54.14',
                'iam': {'info': {
                            u'InstanceProfileArn': u'arn:aws:iam::139485757383:instance-profile/Role',
                            u'InstanceProfileId': u'AIPAIKDEOQWOE343AKRIE',
                            u'Code': u'Success',
                            u'LastUpdated': u'2015-10-21T20:39:13Z'},
                        'security-credentials': {'Role': {
                            u'Code': u'Success',
                            u'LastUpdated': u'2015-10-21T20:39:29Z',
                            u'AccessKeyId': u'ASIAJDAOWE343KFEW33Q',
                            u'SecretAccessKey': u'Re70xjYoIRkQjNZINCYq2cwgyd5UEziwbFyOA5MP',
                            u'Token': u'AQoDYXdzEMb//////////wEa4AMl8nNfsthszPX1KD/3BwONJJv0KwAjt2yKxRPUEXHWRPf+Ruad2gYUtAbQ+1aYjkgaJpRN2z4+7GduYY43XkK/wxSR1hd9TSjEREMORdF3oNXr55kwz9SISOvpQaEKkyCdV11toL6BAkbH/6l+EoLBNFlCLxaBEZfqADEGWAr2XcwSpMLS82AXDcWJ6iX8N7tBZmFe95s3HH+GDkk8MIpfyTDZJDJ56uVUBPKa76ezmZ8bVnDVRE/FfEeYICS4OQCU9EVx7B/xOfQlgE7LlcNjWSyIMnqohEHHzfFVFQyuW/JZDFfT4Pnaan2H4J7udHkoSjV04ctVstAlOd6i4zmeUZjQnTAxiQH4h5TF9Sk2gRaO5XyOAuXrux56Mm30dGLxuljyqVZbVla+YcloiM888+qL8FtfJ4D1yBtnIQZSRi84A23OQP0cILPs6f9mDOR6kxL/ljnSIMM4yHeGuXOTTYxE996fXC6LZAm6jFE91jBnT3iRwzBFQTB7Dq8KsS8c7qUBuZ894qO10pJJqEO+8PcmZCQPenkLu6IdGPaWU9xlpRC8QabKBoIsl1c5fe271uelGLqIEiJ2iwjhF5sxfqQW8CENaLcV5q61YxXBZ9dsH7tAIeYz30Bjyt980lsgo/CfsQU=',
                            u'Expiration': u'2015-10-22T02:48:46Z',
                            u'Type': u'AWS-HMAC'}}}
                }
    metadata.update(kw)
    def f(data='meta-data/', **kw):
        path = [i for i in data.split('/') if i]
        curr = metadata
        assert path[0] == 'meta-data'
        for i in path[1:]:
            curr = curr[i]
        return curr
    return f

@pytest.fixture
def ec2_plugin():
    app = mock.Mock()
    app.database_identifier = '4242'
    app.config = dict()
    from ..ec2 import Ec2Plugin
    return Ec2Plugin('zgres#ec2', app)

@mock.patch('boto.utils.get_instance_metadata')
def test_conn_info(get_instance_metadata, ec2_plugin):
    get_instance_metadata.side_effect = mock_get_instance_metadata()
    ec2_plugin.initialize()
    assert ec2_plugin.get_my_id() == 'i-00000001'
    assert ec2_plugin.get_conn_info() == {
            'ami-id': 'ami-87d579ec',
            'availability-zone': 'us-east-1d',
            'host': '10.1.0.107',
            'instance-type': 't2.micro'}

@pytest.fixture
def ec2_backup_plugin():
    app = mock.Mock()
    app.database_identifier = '4242'
    app.pg_connect_info.return_value = dict(user='postgres', host='example.org')
    from ..ec2 import Ec2SnapshotBackupPlugin
    return Ec2SnapshotBackupPlugin('zgres#ec2-backup', app)

@pytest.mark.xfail
@mock_ec2
@mock.patch('zgres.ec2.psycopg2')
@mock.patch('boto.utils.get_instance_metadata', autospec=True)
@mock.patch('zgres.ec2.LocalDevice', autospec=True)
@mock.patch('zgres.ec2._all_devices_mounted', autospec=True)
def test_ec2_backup_plugin(_all_devices_mounted, local_device, get_instance_metadata, psycopg2, ec2_backup_plugin):
    # setup
    import boto.ec2
    psycopg2.connect().cursor().fetchall.return_value = [['0/2000060']]
    get_instance_metadata.side_effect = mock_get_instance_metadata()
    metadata = get_instance_metadata()
    az = metadata['placement']['availability-zone']
    region = az[:-1]
    conn = boto.ec2.connect_to_region(region)
    reservation = conn.run_instances('ami-12341234')
    instance = reservation.instances[0]
    vol_f = conn.create_volume(10, az)
    vol_f.attach(instance.id, '/dev/sdf')
    vol_g = conn.create_volume(10, az)
    vol_g.attach(instance.id, '/dev/sdg')
    metadata['instance-id'] = instance.id # act as if we are code running on the server we just launched
    # test
    ec2_backup_plugin.app.config = {
            'ec2-snapshot': {
                'dev.1.device': '/dev/sdf',
                'dev.1.iops': '1000',
                'dev.1.size': '300',
                'dev.1.volume_type': 'gp2',
                'dev.2.device': '/dev/sdg',
                }}
    ec2_backup_plugin.initialize()
    # make some snapshots
    ec2_backup_plugin.pg_backup()
    # restore from the snapshots I just made
    ec2_backup_plugin.pg_restore()
