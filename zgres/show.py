import sys
import argparse
from pprint import pprint

from .config import parse_args
from .deadman import App

def show_cli(argv=sys.argv):
    parser = argparse.ArgumentParser(description="Show zgres info")
    config = parse_args(parser, argv)
    if config.get('deadman', {}).get('plugins', '').strip():
        # if deadman is configured show information about it's state
        # HACK, we only need the plugins, really
        plugins = App(config)._plugins
        print('My State:')
        print('    ID: {}'.format(plugins.get_my_id()))
        print('    is replica: {}'.format(plugins.pg_am_i_replica()))
        print('    conn info: {}'.format(plugins.pg_am_i_replica()))
        print('Cluster:')
        print('    database identifier: {}'.format(plugins.dcs_get_database_identifier()))
        print('    timeline: {}'.format(pformat(plugins.dcs_get_timeline())))
        all_state = list(plugins.dcs_get_all_state())
        willing_replicas = plugins.willing_replicas(all_state)
        print('    willing replicas: {}'.format([id for id, _ in willing_replicas]))
        best_replcias = plugins.best_replicas(willing_replicas)
        print('    best replicas: {}'.format([id for id, _ in best_replicas]))
        print('    all conn info: {}'.format(pformat(plugins.dcs_get_all_conn_info())))
        print('    all state: {}'.format(pformat(all_state)))
