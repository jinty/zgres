import sys
from zgres import apply

def _deadman(config):
    raise NotImplementedError()

#
# Command Line Scripts
#

def _parse_args(parser, argv):
    # TODO: add args for setting loglevel here
    args = parser.parse_args(args=argv[1:])
    apply._setup_logging()
    return args

def deadman_cli(argv=sys.argv):
    parser = argparse.ArgumentParser(description="""Monitors/controls the local postgresql installation.

This daemon will do these things:

    - Register the local postgresql instance with Zookeeper by creating a file
      named the IP address to connect on.
    - Try to become master by creating the file:
        master-{cluster_name}
      in zookeeper. If we suceed we create the file /tmp/zgres_become_master. 
    - Shutdown postgres temporarily if we are master and the zookeeper connection is lost.
    - Shutdown postgres permanently if master-{cluster_name} already exists and we didn't create it
        (split-brain avoidance)
    - Monitor the local postgresql installation, if it becomes unavailable,
      withdraw our zookeeper registrations.

It does not:
    - maintain streaming replication (use zgres-apply hooks for that)
    - do remastering (assumed to have happened before we start)
""")
    args = _parse_args(parser, argv)
    config = apply.Config()
    sys.exit(_deadman(config))

