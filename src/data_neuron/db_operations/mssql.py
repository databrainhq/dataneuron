from .base import DatabaseOperations
from .exceptions import ConnectionError, OperationError
from typing import List, Dict, Any, Tuple


class MSSQLOperations(DatabaseOperations):
    def __init__(self, server: str, database: str, username: str, password: str):
        super().__init__()
        self.db_type = "mssql"
        self.conn_params = {
            "server": server,
            "database": database,
            "username": username,
            "password": password
        }
        self.conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}"

    def _get_connection(self):
        try:
            import pyodbc
            return pyodbc.connect(self.conn_str)
        except ImportError:
            raise ConnectionError(
                "MSSQL support is not installed. Please install it with 'pip install your_cli_tool[mssql]'")
        except Exception as e:
            raise ConnectionError(
                f"Failed to connect to MSSQL database: {str(e)}")

    def get_table_list(self) -> List[Dict[str, str]]:
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT TABLE_SCHEMA, TABLE_NAME 
                        FROM INFORMATION_SCHEMA.TABLES 
                        WHERE TABLE_TYPE = 'BASE TABLE'
                    """)
                    return [{"schema": row.TABLE_SCHEMA, "table": row.TABLE_NAME} for row in cursor.fetchall()]
        except Exception as e:
            raise OperationError(f"Failed to get table list: {str(e)}")

    def get_table_info(self, schema: str, table: str) -> Dict[str, Any]:
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"""
                        SELECT 
                            c.COLUMN_NAME, 
                            c.DATA_TYPE,
                            c.IS_NULLABLE,
                            COLUMNPROPERTY(OBJECT_ID('{schema}.{table}'), c.COLUMN_NAME, 'IsIdentity') as IS_IDENTITY,
                            CASE 
                                WHEN pk.COLUMN_NAME IS NOT NULL THEN 'PRI' 
                                ELSE '' 
                            END AS COLUMN_KEY
                        FROM INFORMATION_SCHEMA.COLUMNS c
                        LEFT JOIN (
                            SELECT ku.COLUMN_NAME
                            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
                            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS ku
                                ON tc.CONSTRAINT_TYPE = 'PRIMARY KEY' 
                                AND tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
                            WHERE ku.TABLE_SCHEMA='{schema}'
                            AND ku.TABLE_NAME='{table}'
                        ) pk ON c.COLUMN_NAME = pk.COLUMN_NAME
                        WHERE c.TABLE_SCHEMA = '{schema}'
                        AND c.TABLE_NAME = '{table}'
                    """)
                    columns = cursor.fetchall()
                    return {
                        'schema': schema,
                        'table_name': table,
                        'full_name': f"{schema}.{table}",
                        'columns': [
                            {
                                'name': col.COLUMN_NAME,
                                'type': col.DATA_TYPE,
                                'nullable': col.IS_NULLABLE == 'YES',
                                'primary_key': col.COLUMN_KEY == 'PRI' or col.IS_IDENTITY == 1
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
                    column_names = [column[0] for column in cursor.description]
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
