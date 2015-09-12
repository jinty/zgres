from zgres.apply import Config
from subprocess import check_output

def get_cluster_name(config=None):
    if config is None:
        config = Config()
    return config['environment.json']['db']['name']


def get_neighbours(config=None):
    """Get all nodes in our cluster EXCEPT the node this runs on"""
    if config is None:
        config = Config()

    our_ip4 = check_output(['ec2metadata', '--local-ipv4']).strip()
    our_name = get_cluster_name(config=config)

    for database in config['databases.json']['databases']:
        if database['name'] != our_name:
            continue
        for node in database['nodes']:
            ip = node['ip4']
            if our_ip4 == ip:
                continue
            yield node
    else:
        raise Exception('Could not find our cluster in zookeeper?')
