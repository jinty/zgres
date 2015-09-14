"""Zgres config file"""
import logging
import configparser

def setup_logging(config):
    logging.basicConfig(level=logging.WARN)

def get_config(args):
    config = configparser.ConfigParser()
    config.read(args.config_file)
    return config

def parse_args(parser, argv):
    # TODO: add args for setting loglevel here
    args = parser.parse_args(args=argv[1:])
    config = get_config(args)
    setup_logging(config)
    return config
