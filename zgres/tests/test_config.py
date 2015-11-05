import argparse
import tempfile

import pytest

from zgres import config

def test_read_config_file():
    with tempfile.NamedTemporaryFile() as config_file:
        config_file.write(b'[mysection]\nvalue=42')
        config_file.flush()
        parser = argparse.ArgumentParser()
        args = config.parse_args(parser, ['myscript', '--config', config_file.name], config_file='zgres')
    assert args['mysection']['value'] == '42'

def test_error_with_no_config_file():
    with tempfile.NamedTemporaryFile() as config_file:
        name = config_file.name
    parser = argparse.ArgumentParser()
    with pytest.raises(FileNotFoundError) as exec:
        config.parse_args(parser, ['myscript', '--config', name], config_file='zgres')
