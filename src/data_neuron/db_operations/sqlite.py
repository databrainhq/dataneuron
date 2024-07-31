import sqlite3
from typing import List, Tuple, Dict, Any
from .base import DatabaseOperations
from .exceptions import ConnectionError, OperationError


class SQLiteOperations(DatabaseOperations):
    def __init__(self, db_path):
        super().__init__()
        self.db_type = "sqlite"
        self.db_path = db_path

    def _get_connection(self):
        try:
            return sqlite3.connect(self.db_path)
        except Exception as e:
            raise ConnectionError(
                f"Failed to connect to SQLite database: {str(e)}")

    def get_table_list(self) -> List[Dict[str, str]]:
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table';")
                return [{"schema": "main", "table": table[0]} for table in cursor.fetchall()]
        except Exception as e:
            raise OperationError(f"Failed to get table list: {str(e)}")

    def get_table_info(self, schema: str, table: str) -> Dict[str, Any]:
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(f"PRAGMA table_info({table})")
                columns = cursor.fetchall()
                return {
                    'schema': 'main',
                    'table_name': table,
                    'full_name': f"main.{table}",
                    'columns': [
                        {
                            'name': col[1],
                            'type': col[2],
                            'nullable': col[3] == 0,
                            'primary_key': col[5] == 1
                        } for col in columns
                    ]
                }
        except Exception as e:
            raise OperationError(f"Failed to get table info: {str(e)}")

    def execute_query_with_column_names(self, query: str) -> Tuple[List[Tuple], List[str]]:
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                results = cursor.fetchall()
                column_names = [description[0]
                                for description in cursor.description]
                return results, column_names
        except Exception as e:
            raise OperationError(f"Failed to execute query: {str(e)}")

    def execute_query(self, query: str) -> List[Tuple]:
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                results = cursor.fetchall()
                return results
        except Exception as e:
            raise OperationError(f"Failed to execute query: {str(e)}")
