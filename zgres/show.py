import sys
import argparse
from pprint import pformat, pprint

from .config import parse_args
from .deadman import App

def show_cli(argv=sys.argv):
    parser = argparse.ArgumentParser(description="Show zgres info")
    config = parse_args(parser, argv, config_file='deadman.ini')
    if config.has_section('deadman') and config['deadman'].get('plugins', '').strip():
        # if deadman is configured show information about it's state
        # HACK, we only need the plugins, really
        plugins = App(config)._plugins
        plugins.initialize()
        print('My State:')
        print('    ID: {}'.format(plugins.get_my_id()))
        print('    role: {}'.format(plugins.pg_replication_role()))
        print('Cluster:')
        print('    current master: {}'.format(plugins.dcs_get_lock_owner('master')))
        print('    database identifier: {}'.format(plugins.dcs_get_database_identifier()))
        print('    timeline: {}'.format(pformat(plugins.dcs_get_timeline())))
        all_state = list(plugins.dcs_list_state())
        willing_replicas = list(plugins.willing_replicas(all_state))
        print('    willing replicas:')
        pprint(willing_replicas)
        best_replicas = list(plugins.best_replicas(willing_replicas))
        print('    best replicas:')
        pprint(best_replicas)
        print('    all conn info:')
        pprint(list(plugins.dcs_list_conn_info()))
        print('    all state:')
        pprint(all_state)
