"""Zgres config file"""
import os
import logging
import configparser

def _add_common_args(parser):
    parser.add_argument('-c', '--config-file',
            dest='config_file',
            default='/etc/zgres/zgres.ini',
            help='Use this config file')

def _setup_logging(config):
    logging.basicConfig(level=logging.WARN)

def _get_config(args):
    config = configparser.ConfigParser()
    if not os.path.exists(args.config_file):
        raise AssertionError('Config file {} does not exist'.format(args.config_file))
    config.read(args.config_file)
    return config

def parse_args(parser, argv):
    # TODO: add args for setting loglevel here
    _add_common_args(parser)
    args = parser.parse_args(args=argv[1:])
    config = _get_config(args)
    _setup_logging(config)
    return config
