from .sqlite import SQLiteOperations


class DatabaseFactory:
    @staticmethod
    def get_database(db_type: str, **kwargs):
        if db_type == 'sqlite':
            return SQLiteOperations(kwargs.get('db_path'))
        elif db_type == 'postgres':
            from .postgres import PostgreSQLOperations
            return PostgreSQLOperations(
                dbname=kwargs.get('dbname'),
                user=kwargs.get('user'),
                password=kwargs.get('password'),
                host=kwargs.get('host'),
                port=kwargs.get('port')
            )
        elif db_type == 'mysql':
            from .mysql import MySQLOperations
            return MySQLOperations(
                host=kwargs.get('host'),
                user=kwargs.get('user'),
                password=kwargs.get('password'),
                database=kwargs.get('database')
            )
        elif db_type == 'mssql':
            from .mssql import MSSQLOperations
            return MSSQLOperations(
                server=kwargs.get('server'),
                database=kwargs.get('database'),
                username=kwargs.get('username'),
                password=kwargs.get('password')
            )
        else:
            raise ValueError(f"Unsupported database type: {db_type}")

# db = DatabaseFactory.get_database('mssql',
#                                   server='your_server_name',
#                                   database='your_database_name',
#                                   username='your_username',
#                                   password='your_password')
