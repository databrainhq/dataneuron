import yaml
import os
from .sqlite import SQLiteOperations
from .exceptions import ConfigurationError, ConnectionError

CONFIG_PATH = 'database.yaml'


class DatabaseFactory:
    @staticmethod
    def get_database():
        try:
            db_config = DatabaseFactory.load_config()
            db_type = db_config.get('name')

            if db_type == 'sqlite':
                return SQLiteOperations(db_config.get('db_path'))
            elif db_type == 'postgres':
                from .postgres import PostgreSQLOperations
                return PostgreSQLOperations(
                    dbname=db_config.get('dbname'),
                    user=db_config.get('user'),
                    password=db_config.get('password'),
                    host=db_config.get('host'),
                    port=db_config.get('port')
                )
            elif db_type == 'mysql':
                from .mysql import MySQLOperations
                return MySQLOperations(
                    host=db_config.get('host'),
                    user=db_config.get('user'),
                    password=db_config.get('password'),
                    database=db_config.get('database')
                )
            elif db_type == 'mssql':
                from .mssql import MSSQLOperations
                return MSSQLOperations(
                    server=db_config.get('server'),
                    database=db_config.get('database'),
                    username=db_config.get('username'),
                    password=db_config.get('password')
                )
            else:
                raise ConfigurationError(
                    f"Unsupported database type: {db_type}")
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
                config = yaml.safe_load(file)
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
