from .base import DatabaseOperations
from .exceptions import ConnectionError, OperationError
from typing import List, Tuple, Dict, Any


class MySQLOperations(DatabaseOperations):
    def __init__(self, host: str, user: str, password: str, database: str):
        super().__init__()
        self.db_type = "mysql"
        self.conn_params = {
            "host": host,
            "user": user,
            "password": password,
            "database": database
        }

    def _get_connection(self):
        try:
            import mysql.connector
            return mysql.connector.connect(**self.conn_params)
        except ImportError:
            raise ConnectionError(
                "MySQL support is not installed. Please install it with 'pip install your_cli_tool[mysql]'")
        except Exception as e:
            raise ConnectionError(
                f"Failed to connect to MySQL database: {str(e)}")

    def get_table_list(self) -> List[Dict[str, str]]:
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SHOW TABLES")
                    return [{"schema": self.conn_params['database'], "table": table[0]} for table in cursor.fetchall()]
        except Exception as e:
            raise OperationError(f"Failed to get table list: {str(e)}")

    def get_table_info(self, schema: str, table: str) -> Dict[str, Any]:
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"DESCRIBE {table}")
                    columns = cursor.fetchall()
                    return {
                        'schema': schema,
                        'table_name': table,
                        'full_name': f"{schema}.{table}",
                        'columns': [
                            {
                                'name': col[0],
                                'type': col[1],
                                'nullable': col[2] == 'YES',
                                'primary_key': col[3] == 'PRI'
                            } for col in columns
                        ]
                    }
        except Exception as e:
            raise OperationError(f"Failed to get table info: {str(e)}")

    def execute_query_with_column_names(self, query: str) -> Tuple[List[Tuple], List[str]]:
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    results = cursor.fetchall()
                    column_names = [desc[0] for desc in cursor.description]
                    return results, column_names
        except Exception as e:
            raise OperationError(f"Failed to execute query: {str(e)}")

    def execute_query(self, query: str) -> List[Tuple]:
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    return cursor.fetchall()
        except Exception as e:
            raise OperationError(f"Failed to execute query: {str(e)}")
