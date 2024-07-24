import yaml
import os
from .sqlite import SQLiteOperations


CONFIG_PATH = 'database.yaml'


class DatabaseFactory:
    @staticmethod
    def get_database():
        if not os.path.exists(CONFIG_PATH):
            raise FileNotFoundError(
                f"Configuration file '{CONFIG_PATH}' not found. Please run the db-init command first.")

        with open(CONFIG_PATH, 'r') as file:
            config = yaml.safe_load(file)

        db_config = config.get('database', {})
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
            raise ValueError(f"Unsupported database type: {db_type}")

    @staticmethod
    def load_config():
        if not os.path.exists(CONFIG_PATH):
            raise FileNotFoundError(
                f"Configuration file '{CONFIG_PATH}' not found. Please run the db-init command first.")

        with open(CONFIG_PATH, 'r') as file:
            return yaml.safe_load(file).get('database', {})
