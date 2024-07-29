# File: db_init_cmd.py

import click
import yaml
import os
from ..utils.print import print_success, print_info, confirm_with_user


def get_db_config(db_type):
    config = {'database': {'name': db_type}}

    use_env = confirm_with_user(
        "Do you want to use environment variables for configuration?")

    if use_env:
        if db_type == 'sqlite':
            config['database']['db_path'] = '${SQLITE_DB_PATH}'
        elif db_type == 'postgres':
            config['database'].update({
                'dbname': '${POSTGRES_DB}',
                'user': '${POSTGRES_USER}',
                'password': '${POSTGRES_PASSWORD}',
                'host': '${POSTGRES_HOST}',
                'port': '${POSTGRES_PORT}'
            })
        elif db_type == 'mysql':
            config['database'].update({
                'host': '${MYSQL_HOST}',
                'user': '${MYSQL_USER}',
                'password': '${MYSQL_PASSWORD}',
                'database': '${MYSQL_DATABASE}'
            })
        elif db_type == 'mssql':
            config['database'].update({
                'server': '${MSSQL_SERVER}',
                'database': '${MSSQL_DATABASE}',
                'username': '${MSSQL_USERNAME}',
                'password': '${MSSQL_PASSWORD}'
            })
        elif db_type == 'csv':
            config['database']['data_directory'] = '${DATA_DIRECTORY}'
        elif db_type == 'clickhouse':
            config['database'].update({
                'host': '${CLICKHOUSE_HOST}',
                'port': '${CLICKHOUSE_PORT}',
                'user': '${CLICKHOUSE_USER}',
                'password': '${CLICKHOUSE_PASSWORD}'
            })
            if confirm_with_user("Does your ClickHouse.cloud setup use a specific database?"):
                config['database']['database'] = '${CLICKHOUSE_DATABASE}'

        print_info("Please set the following environment variables:")
        for key, value in config['database'].items():
            if value.startswith('${'):
                click.echo(f"  {value[2:-1]}=<your_{key}_here>")
    else:
        if db_type == 'sqlite':
            config['database']['db_path'] = click.prompt(
                "Enter the SQLite database path", type=str)
        elif db_type == 'postgres':
            config['database'].update({
                'dbname': click.prompt("Enter the database name", type=str),
                'user': click.prompt("Enter the username", type=str),
                'password': click.prompt("Enter the password", type=str, hide_input=True),
                'host': click.prompt("Enter the host", type=str, default='localhost'),
                'port': click.prompt("Enter the port", type=int, default=5432)
            })
        elif db_type == 'mysql':
            config['database'].update({
                'host': click.prompt("Enter the host", type=str, default='localhost'),
                'user': click.prompt("Enter the username", type=str),
                'password': click.prompt("Enter the password", type=str, hide_input=True),
                'database': click.prompt("Enter the database name", type=str)
            })
        elif db_type == 'mssql':
            config['database'].update({
                'server': click.prompt("Enter the server name", type=str),
                'database': click.prompt("Enter the database name", type=str),
                'username': click.prompt("Enter the username", type=str),
                'password': click.prompt("Enter the password", type=str, hide_input=True)
            })
        elif db_type == 'csv':
            config['database']['data_directory'] = click.prompt(
                "Enter the directory path containing your CSV/Parquet files", type=str)
        elif db_type == 'clickhouse':
            config['database'].update({
                'host': click.prompt("Enter the host", type=str),
                'port': click.prompt("Enter the port", type=int, default=8443),
                'user': click.prompt("Enter the username", type=str),
                'password': click.prompt("Enter the password", type=str, hide_input=True)
            })
            if confirm_with_user("Does your ClickHouse.cloud setup use a specific database?"):
                config['database']['database'] = click.prompt(
                    "Enter the database name", type=str)

    return config


def init_database_config(db_type):
    config = get_db_config(db_type)

    with open('database.yaml', 'w') as f:
        yaml.dump(config, f, default_flow_style=False)

    print_success(
        f"Database configuration for {db_type} has been saved to database.yaml")
