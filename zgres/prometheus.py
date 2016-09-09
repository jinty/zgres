import sys
import logging
import argparse
from time import sleep

from prometheus_client import Gauge, start_http_server

from .config import parse_args
from .deadman import App, willing_replicas

metric_dcs_has_conn_info = Gauge('zgres_dcs_has_conn_info', '1 if the server is "connectable" i.e. has conn_info in the DCS, else 0')
metric_dcs_is_willing_replica = Gauge('zgres_is_willing_replica', '1 if the server is "willing_to_take_over" from the master, else 0')
metric_dcs_is_master = Gauge('zgres_is_master', '1 if the server is the current master, else 0')
metric_dcs_willing_since = Gauge('zgres_willing_since', 'Timestamp since which this server has been willing to take over')

def deadman_exporter(argv=sys.argv):
    """This daemon monitors the local zgres-deadman daemon running on this machine.

    It works by using the deadman configuration to look into the DCS to find
    statistics for this machine. We build it as a separate daemon to lessen the
    risk that monitoring and statistics collection inside the zgres-deadman
    will cause errors.

    We run it on the same machine as this provides:
        * reusability of the existing deadman configuration
        * easier prometheus configuration
        * automatic HA
    """
    parser = argparse.ArgumentParser(description="Prometheus statistics daemon for zgres-deadman")
    config = parse_args(parser, argv, config_file='deadman.ini')
    # this sleep prevents us from restarting too fast and systemd failing to restart us
    # we use a fail-always architecture here, any exception causes a daemon restart
    sleep(10)
    start_http_server(9163)
    # use only one plugin and zookeeper connection, otherwise we get memory leaks :(
    plugins = App(config)._plugins
    plugins.initialize()
    while True:
        dcs_has_conn_info = 0
        dcs_is_willing_replica = 0
        # HACK, we only need the plugins, really
        all_state = list(plugins.dcs_list_state())
        my_id = plugins.get_my_id()
        for id, state in all_state:
            if id == my_id:
                if 'master' == state.get('replication_role'):
                    metric_dcs_is_master.set(1)
                else:
                    metric_dcs_is_master.set(0)
                break
        for id, state in willing_replicas(all_state):
            if id == my_id:
                dcs_is_willing_replica = 1
                metric_dcs_willing_since.set(state['willing'])
                break
        for id, conn_info in plugins.dcs_list_conn_info():
            if id == my_id:
                dcs_has_conn_info = 1
                break
        metric_dcs_has_conn_info.set(dcs_has_conn_info)
        metric_dcs_is_willing_replica.set(dcs_is_willing_replica)
        sleep(60)

if __name__ == '__main__':
    deadman_exporter()
