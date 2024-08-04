from .base import DatabaseOperations
from .exceptions import ConnectionError, OperationError
from typing import List, Tuple, Dict, Any


class ClickHouseOperations(DatabaseOperations):
    def __init__(self, host: str, port: int, user: str, password: str, database: str):
        super().__init__()
        self.db_type = "clickhouse"
        self.conn_params = {
            "host": host,
            "port": port,
            "user": user,
            "password": password,
        }
        if database:
            self.conn_params["database"] = database

    def _get_connection(self):
        try:
            import clickhouse_connect
            return clickhouse_connect.get_client(**self.conn_params)
        except ImportError:
            raise ConnectionError(
                "ClickHouse support is not installed. Please install it with 'pip install your_cli_tool[clickhouse]'")
        except Exception as e:
            raise ConnectionError(
                f"Failed to connect to ClickHouse database: {str(e)}")

    def get_table_list(self) -> List[Dict[str, str]]:
        try:
            client = self._get_connection()
            result = client.query("SHOW TABLES")
            return [{"schema": self.conn_params.get('database', 'default'), "table": table[0]} for table in result.result_rows]
        except Exception as e:
            raise OperationError(f"Failed to get table list: {str(e)}")

    def get_table_info(self, schema: str, table: str) -> Dict[str, Any]:
        try:
            client = self._get_connection()
            result = client.query(f"DESCRIBE {schema}.{table}")
            return {
                'schema': schema,
                'table_name': table,
                'full_name': f"{schema}.{table}",
                'columns': [
                    {
                        'name': col[0],
                        'type': col[1],
                        'nullable': col[5] == 'YES',
                        # ClickHouse uses 'key' to indicate primary key columns
                        'primary_key': col[2] == 'key'
                    } for col in result.result_rows
                ]
            }
        except Exception as e:
            raise OperationError(f"Failed to get table info: {str(e)}")

    def execute_query_with_column_names(self, query: str) -> Tuple[List[Tuple], List[str]]:
        try:
            client = self._get_connection()
            result = client.query(query)
            return result.result_rows, result.column_names
        except Exception as e:
            raise OperationError(f"Failed to execute query: {str(e)}")

    def execute_query(self, query: str) -> List[Tuple]:
        try:
            client = self._get_connection()
            result = client.query(query)
            return result.result_rows
        except Exception as e:
            raise OperationError(f"Failed to execute query: {str(e)}")
