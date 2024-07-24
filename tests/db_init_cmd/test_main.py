# File: tests/test_db_init_cmd.py

import pytest
from unittest.mock import patch
from src.data_neuron.db_init_cmd.main import get_db_config


@pytest.fixture
def mock_confirm(monkeypatch):
    def mock_confirm_with_user(*args, **kwargs):
        return True
    monkeypatch.setattr(
        'src.data_neuron.db_init_cmd.main.confirm_with_user', mock_confirm_with_user)


@pytest.fixture
def mock_env_vars(monkeypatch):
    env_vars = {
        'SQLITE_DB_PATH': '/path/to/sqlite.db',
        'POSTGRES_DB': 'test_db',
        'POSTGRES_USER': 'test_user',
        'POSTGRES_PASSWORD': 'test_pass',
        'POSTGRES_HOST': 'localhost',
        'POSTGRES_PORT': '5432',
        'MYSQL_HOST': 'localhost',
        'MYSQL_USER': 'mysql_user',
        'MYSQL_PASSWORD': 'mysql_pass',
        'MYSQL_DATABASE': 'mysql_db',
        'MSSQL_SERVER': 'mssql_server',
        'MSSQL_DATABASE': 'mssql_db',
        'MSSQL_USERNAME': 'mssql_user',
        'MSSQL_PASSWORD': 'mssql_pass'
    }
    for key, value in env_vars.items():
        monkeypatch.setenv(key, value)


def test_get_db_config_sqlite_env(mock_confirm, mock_env_vars):
    config = get_db_config('sqlite')
    assert config['database']['name'] == 'sqlite'
    assert config['database']['db_path'] == '${SQLITE_DB_PATH}'


def test_get_db_config_postgres_env(mock_confirm, mock_env_vars):
    config = get_db_config('postgres')
    assert config['database']['name'] == 'postgres'
    assert config['database']['dbname'] == '${POSTGRES_DB}'
    assert config['database']['user'] == '${POSTGRES_USER}'
    assert config['database']['password'] == '${POSTGRES_PASSWORD}'
    assert config['database']['host'] == '${POSTGRES_HOST}'
    assert config['database']['port'] == '${POSTGRES_PORT}'


def test_get_db_config_mysql_env(mock_confirm, mock_env_vars):
    config = get_db_config('mysql')
    assert config['database']['name'] == 'mysql'
    assert config['database']['host'] == '${MYSQL_HOST}'
    assert config['database']['user'] == '${MYSQL_USER}'
    assert config['database']['password'] == '${MYSQL_PASSWORD}'
    assert config['database']['database'] == '${MYSQL_DATABASE}'


def test_get_db_config_mssql_env(mock_confirm, mock_env_vars):
    config = get_db_config('mssql')
    assert config['database']['name'] == 'mssql'
    assert config['database']['server'] == '${MSSQL_SERVER}'
    assert config['database']['database'] == '${MSSQL_DATABASE}'
    assert config['database']['username'] == '${MSSQL_USERNAME}'
    assert config['database']['password'] == '${MSSQL_PASSWORD}'


@patch('src.data_neuron.db_init_cmd.main.confirm_with_user', return_value=False)
@patch('src.data_neuron.db_init_cmd.main.click.prompt')
def test_get_db_config_manual_input(mock_prompt, mock_confirm):
    mock_prompt.side_effect = ['/path/to/manual.db']
    config = get_db_config('sqlite')
    assert config['database']['name'] == 'sqlite'
    assert config['database']['db_path'] == '/path/to/manual.db'


@patch('src.data_neuron.db_init_cmd.main.confirm_with_user', return_value=False)
@patch('src.data_neuron.db_init_cmd.main.click.prompt')
def test_get_db_config_postgres_manual(mock_prompt, mock_confirm):
    mock_prompt.side_effect = ['test_db',
                               'test_user', 'test_pass', 'localhost', 5432]
    config = get_db_config('postgres')
    assert config['database']['name'] == 'postgres'
    assert config['database']['dbname'] == 'test_db'
    assert config['database']['user'] == 'test_user'
    assert config['database']['password'] == 'test_pass'
    assert config['database']['host'] == 'localhost'
    assert config['database']['port'] == 5432
