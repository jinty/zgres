import sys
import argparse
from pprint import pformat, pprint

from .config import parse_args
from .deadman import App, willing_replicas

def indented_pprint(obj):
    lines = []
    for line in pformat(obj).splitlines(True):
        lines.append('    ')
        lines.append(line)
    print(''.join(lines))

def show_cli(argv=sys.argv):
    parser = argparse.ArgumentParser(description="Show zgres info")
    config = parse_args(parser, argv, config_file='deadman.ini')
    if config.has_section('deadman') and config['deadman'].get('plugins', '').strip():
        plugins = App(config)._plugins
        plugins.initialize()
        all_state = list(plugins.dcs_list_state())
        my_id = plugins.get_my_id()
        my_state = None
        for id, state in all_state:
            if id == my_id:
                my_state = state
                break
        # if deadman is configured show information about it's state
        # HACK, we only need the plugins, really
        print('My State:')
        print('    ID: {}'.format(my_id))
        if my_state is None:
            role = 'not registered in zookeeper'
        else:
            role = my_state.get('replication_role')
        print('    Replication role: {}'.format(role))
        print('Cluster:')
        print('    current master: {}'.format(plugins.dcs_get_lock_owner(name='master')))
        print('    database identifier: {}'.format(plugins.dcs_get_database_identifier()))
        print('    timeline: {}'.format(pformat(plugins.dcs_get_timeline())))
        # willing_replicas is removed!
        willing = list(willing_replicas(all_state))
        print('\nwilling replicas:')
        indented_pprint(willing)
        best_replicas = list(plugins.best_replicas(states=willing))
        print('\nbest replicas:')
        indented_pprint(best_replicas)
        print('\nall conn info:')
        indented_pprint(list(plugins.dcs_list_conn_info()))
        print('\nall state:')
        indented_pprint(all_state)
