import os
from subprocess import check_call, call

def write_service(service_name, contents):
    """Write a service file in a "safe" manner.
    
    If the contents of the file are the same as what is desired to be written,
    do nothing.

    First writes to a temporary file in the same directory as the target, them
    move that temporary file into plce.

    Return a boolean True if the file was changed else False
    """
    assert '/' not in service_name
    path = '/lib/systemd/system/' + service_name
    if os.path.exists(path):
        with open(path, 'r') as f:
            existing = f.read()
        if existing == contents:
            return False
    tmppath = '/lib/systemd/system/.' + service_name + '.tmp'
    with open(tmppath, 'w') as f:
        f.write(contents)
    os.rename(tmppath, path)
    return True
    
def assert_enabled_and_running(service_name, reload=False, reload_daemon=False, restart=False):
    check_call(['systemctl', 'enable', service_name])
    if reload_daemon:
        check_call(['systemctl', 'daemon-reload'])
    check_call(['systemctl', 'start', service_name]) # do we need to check status?
    if reload:
        check_call(['systemctl', 'reload', service_name]) # maybe avoid if we just started the service
    if restart:
        check_call(['systemctl', 'restart', service_name]) # maybe avoid if we just started the service

def assert_disabled_and_stopped(service_name):
    check_call(['systemctl', 'disable', service_name])
    call(['systemctl', 'stop', service_name]) # fails if service does not exist
