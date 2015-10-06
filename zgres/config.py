"""Zgres config file"""
import os
import logging
import configparser

def _add_common_args(parser):
    parser.add_argument('-c', '--config',
            dest='config',
            nargs='*',
            default=['/etc/zgres/zgres.ini', '/etc/zgres/zgres.ini.d'],
            help='Use this config file or directory. If a directory, all files ending with .ini are parsed. Order is important with latter files over-riding earlier ones.')

def _setup_logging(config):
    # TODO: send INFO and maybe DEBUG to stdout
    # ERROR and WARN to stderr
    logging.basicConfig(level=logging.INFO)

def _get_config(args):
    config = configparser.ConfigParser()
    for file_or_dir in args.config:
        if os.path.isfile(file_or_dir):
            config.read_file(open(file_or_dir, 'r'))
        else:
            for cfg in sorted(os.listdir(file_or_dir)):
                if cfg.startswith('.') or not cfg.endswith('.ini'):
                    continue
                config.read_file(open(os.path.join(file_or_dir, cfg), 'r'))
    return config

def parse_args(parser, argv):
    # TODO: add args for setting loglevel here
    _add_common_args(parser)
    args = parser.parse_args(args=argv[1:])
    config = _get_config(args)
    _setup_logging(config)
    return config
