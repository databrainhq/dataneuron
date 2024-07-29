import os
from typing import List, Dict, Any, Tuple
from .base import DatabaseOperations
from .exceptions import ConnectionError, OperationError


class DuckDBOperations(DatabaseOperations):
    def __init__(self, data_directory: str):
        super().__init__()
        self.db_type = "duckdb"
        self.data_directory = os.path.expanduser(data_directory)
        self.conn = None
        self.csv_files = []

    def _get_connection(self):
        if not self.conn:
            try:
                import duckdb
                self.conn = duckdb.connect(database=':memory:')
                self._load_csv_files()
            except ImportError as e:
                raise ConnectionError("DuckDB support is not installed. "
                                      "Please install it with 'pip install duckdb'") from e
            except Exception as e:
                raise ConnectionError(
                    f"Failed to connect to DuckDB: {str(e)}") from e
        return self.conn

    def _load_csv_files(self):
        try:
            self.csv_files = [f for f in os.listdir(
                self.data_directory) if f.endswith('.csv')]
            for csv_file in self.csv_files:
                table_name = os.path.splitext(csv_file)[0]
                file_path = os.path.join(self.data_directory, csv_file)
                self.conn.execute(f"""
                    CREATE TABLE {table_name} AS 
                    SELECT * FROM read_csv_auto('{file_path}', ALL_VARCHAR=1)
                """)
        except Exception as e:
            raise OperationError(f"Failed to load CSV files: {str(e)}") from e

    def get_table_list(self) -> List[Dict[str, str]]:
        try:
            conn = self._get_connection()
            result = conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").fetchall()
            return [{"schema": "main", "table": row[0]} for row in result]
        except Exception as e:
            raise OperationError(f"Failed to get table list: {str(e)}") from e

    def get_table_info(self, schema: str, table: str) -> Dict[str, Any]:
        try:
            conn = self._get_connection()
            columns = conn.execute(f"PRAGMA table_info('{table}')").fetchall()
            return {
                'schema': 'main',
                'table_name': table,
                'full_name': f"main.{table}",
                'columns': [
                    {
                        'name': col[1],
                        'type': col[2],
                        'nullable': not col[3],
                        'primary_key': col[5] == 1
                    } for col in columns
                ]
            }
        except Exception as e:
            raise OperationError(f"Failed to get table info: {str(e)}") from e

    def execute_query_with_column_names(self, query: str) -> Tuple[List[Tuple], List[str]]:
        try:
            conn = self._get_connection()
            result = conn.execute(query)
            column_names = [desc[0] for desc in result.description]
            results = result.fetchall()
            return results, column_names
        except Exception as e:
            raise OperationError(f"Failed to execute query: {str(e)}") from e

    def execute_query(self, query: str) -> List[Tuple]:
        try:
            conn = self._get_connection()
            result = conn.execute(query).fetchall()
            return result
        except Exception as e:
            raise OperationError(f"Failed to execute query: {str(e)}")
