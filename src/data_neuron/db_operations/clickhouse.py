from .base import DatabaseOperations
from .exceptions import ConnectionError, OperationError
from typing import List, Tuple


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

    def get_table_list(self) -> List[str]:
        try:
            client = self._get_connection()
            result = client.query("SHOW TABLES")
            return [row[0] for row in result.result_rows]
        except Exception as e:
            raise OperationError(f"Failed to get table list: {str(e)}")

    def get_table_info(self, table_name: str) -> dict:
        try:
            client = self._get_connection()
            result = client.query(f"DESCRIBE {table_name}")
            return {
                'table_name': table_name,
                'columns': [
                    {
                        'name': col[0],
                        'type': col[1],
                        'nullable': col[5] == 'YES',
                        'primary_key': col[6] == 'true'
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

    def execute_query(self, query: str) -> str:
        try:
            client = self._get_connection()
            result = client.query(query)
            return result.result_rows
        except Exception as e:
            raise OperationError(f"Failed to execute query: {str(e)}")

    def get_schema_info(self) -> str:
        try:
            client = self._get_connection()
            tables = self.get_table_list()
            schema_info = []
            for table in tables:
                schema_info.append(f"\nTable: {table}")
                columns = client.query(f"DESCRIBE {table}")
                for column in columns.result_rows:
                    schema_info.append(f"  {column[0]} ({column[1]})")
            return "\n".join(schema_info)
        except Exception as e:
            raise OperationError(f"Failed to get schema info: {str(e)}")
