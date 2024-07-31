import yaml
import os
import re
from .sqlite import SQLiteOperations
from .exceptions import ConfigurationError, ConnectionError

CONFIG_PATH = 'database.yaml'


class DatabaseFactory:
    @staticmethod
    def get_database(db_config=None):
        try:
            if db_config is None:
                db_config = DatabaseFactory.load_config()

            db_type = db_config.get('name')
            db = None

            if db_type == 'sqlite':
                db = SQLiteOperations(db_config.get('db_path'))
            elif db_type == 'postgres':
                from .postgres import PostgreSQLOperations
                db = PostgreSQLOperations(
                    dbname=db_config.get('dbname'),
                    user=db_config.get('user'),
                    password=db_config.get('password'),
                    host=db_config.get('host'),
                    port=db_config.get('port')
                )
            elif db_type == 'mysql':
                from .mysql import MySQLOperations
                db = MySQLOperations(
                    host=db_config.get('host'),
                    user=db_config.get('user'),
                    password=db_config.get('password'),
                    database=db_config.get('database')
                )
            elif db_type == 'mssql':
                from .mssql import MSSQLOperations
                db = MSSQLOperations(
                    server=db_config.get('server'),
                    database=db_config.get('database'),
                    username=db_config.get('username'),
                    password=db_config.get('password')
                )
            elif db_type == 'csv':
                from .duckdb import DuckDBOperations
                db = DuckDBOperations(db_config.get('data_directory'))
            elif db_type == 'clickhouse':
                from .clickhouse import ClickHouseOperations
                db = ClickHouseOperations(
                    host=db_config.get('host'),
                    port=db_config.get('port'),
                    user=db_config.get('user'),
                    password=db_config.get('password'),
                    database=db_config.get('database')
                )
            else:
                raise ConfigurationError(
                    f"Unsupported database type: {db_type}")

            if db:
                db.db_type = db_type
                return db
            else:
                raise ConfigurationError("Failed to create database object")
        except ImportError as e:
            raise ConfigurationError(f"Failed to import database module: {str(e)}. "
                                     f"Make sure you have installed the necessary dependencies.")
        except Exception as e:
            raise ConnectionError(
                f"Failed to create database connection: {str(e)}")

    @staticmethod
    def load_config():
        if not os.path.exists(CONFIG_PATH):
            raise ConfigurationError(
                f"Configuration file '{CONFIG_PATH}' not found. Please run the db-init command first.")
        try:
            with open(CONFIG_PATH, 'r') as file:
                config_content = file.read()

            # Replace environment variables
            config_content = DatabaseFactory.replace_env_vars(config_content)

            config = yaml.safe_load(config_content)
            db_config = config.get('database', {})
            if not db_config:
                raise ConfigurationError(
                    "No database configuration found in the YAML file.")
            return db_config
        except yaml.YAMLError as e:
            raise ConfigurationError(
                f"Error parsing YAML configuration: {str(e)}")
        except Exception as e:
            raise ConfigurationError(f"Error loading configuration: {str(e)}")

    @staticmethod
    def replace_env_vars(content):
        pattern = re.compile(r'\$\{([^}^{]+)\}')

        def replace(match):
            env_var = match.group(1)
            value = os.environ.get(env_var)
            if value is None:
                raise ConfigurationError(
                    f"Environment variable '{env_var}' is not set")
            return value
        return pattern.sub(replace, content)
