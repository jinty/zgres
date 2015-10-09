"""Zgres config file"""
import os
import sys
import logging
import configparser

class StdOutFilter(logging.Filter):

    def filter(self, record):
        return record.levelno < logging.WARNING

def _add_common_args(parser):
    parser.add_argument('-c', '--config',
            dest='config',
            nargs='*',
            default=['/etc/zgres/zgres.ini', '/etc/zgres/zgres.ini.d'],
            help='Use this config file or directory. If a directory, all files ending with .ini are parsed. Order is important with latter files over-riding earlier ones.')

def _setup_logging(config):
    # TODO: a --debug option which prints DEBUG messages
    # TODO: a --quiet option which hides the INFO messages
    root_logger = logging.getLogger()
    # less than WARN to stderr
    stdout = logging.StreamHandler(sys.stdout)
    stdout.addFilter(StdOutFilter())
    root_logger.addHandler(stdout)
    # WARN and above to stderr
    stderr = logging.StreamHandler(sys.stderr)
    stderr.setLevel(logging.WARNING)
    root_logger.addHandler(stderr)

def _get_config(args):
    config = configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())
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
    _add_common_args(parser)
    args = parser.parse_args(args=argv[1:])
    config = _get_config(args)
    _setup_logging(config)
    return config
