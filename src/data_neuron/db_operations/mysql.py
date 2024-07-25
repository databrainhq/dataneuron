from .base import DatabaseOperations
from .exceptions import OperationError
from typing import List, Tuple


class MySQLOperations(DatabaseOperations):
    def __init__(self, host: str, user: str, password: str, database: str):
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
            raise ImportError(
                "MySQL support is not installed. Please install it with 'pip install your_cli_tool[mysql]'")

    def get_table_list(self):
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SHOW TABLES")
                    return [table[0] for table in cursor.fetchall()]
        except Exception as e:
            return f"An error occurred: {str(e)}"

    def get_table_info(self, table_name):
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"DESCRIBE {table_name}")
                    columns = cursor.fetchall()
                    return {
                        'table_name': table_name,
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
            return f"An error occurred: {str(e)}"

    def execute_query_with_column_names(self, query: str) -> Tuple[List[Tuple], List[str]]:
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    results = cursor.fetchall()
                    column_names = [desc[0] for desc in cursor.description]
                    return results, column_names
        except Exception as e:
            raise OperationError(f"Failed to execute query: {str(e)}") from e

    def execute_query(self, query: str) -> str:
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    results = cursor.fetchall()
                    return "\n".join([str(row) for row in results])
        except Exception as e:
            return f"An error occurred: {str(e)}"

    def get_schema_info(self) -> str:
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SHOW TABLES")
                    tables = cursor.fetchall()
                    schema_info = []
                    for table in tables:
                        table_name = table[0]
                        cursor.execute(f"DESCRIBE {table_name}")
                        columns = cursor.fetchall()
                        schema_info.append(f"\nTable: {table_name}")
                        for column in columns:
                            schema_info.append(f"  {column[0]} ({column[1]})")
                    return "\n".join(schema_info)
        except Exception as e:
            return f"An error occurred: {str(e)}"
