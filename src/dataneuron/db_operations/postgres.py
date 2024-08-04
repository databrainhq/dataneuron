from .base import DatabaseOperations
from .exceptions import ConnectionError, OperationError
from typing import List, Tuple, Dict, Any


class PostgreSQLOperations(DatabaseOperations):
    def __init__(self, dbname: str, user: str, password: str, host: str, port: str):
        super().__init__()
        self.db_type = "postgres"
        self.conn_params = {
            "dbname": dbname,
            "user": user,
            "password": password,
            "host": host,
            "port": port
        }

    def _get_connection(self):
        try:
            import psycopg2
            return psycopg2.connect(**self.conn_params)
        except ImportError as e:
            raise ConnectionError("PostgreSQL support is not installed. "
                                  "Please install it with 'pip install your_cli_tool[postgres]'") from e
        except Exception as e:
            raise ConnectionError(
                f"Failed to connect to PostgreSQL database: {str(e)}") from e

    def get_table_list(self) -> List[Dict[str, str]]:
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT table_schema, table_name
                        FROM information_schema.tables
                        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                        AND table_type = 'BASE TABLE'
                        ORDER BY table_schema, table_name
                    """)
                    return [{"schema": row[0], "table": row[1]} for row in cursor.fetchall()]
        except Exception as e:
            raise OperationError(f"Failed to get table list: {str(e)}") from e

    def get_table_info(self, schema: str, table: str) -> Dict[str, Any]:
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"""
                        SELECT 
                            c.column_name, 
                            c.data_type,
                            c.is_nullable,
                            CASE WHEN pk.column_name IS NOT NULL THEN TRUE ELSE FALSE END AS is_primary_key
                        FROM information_schema.columns c
                        LEFT JOIN (
                            SELECT ku.column_name
                            FROM information_schema.table_constraints tc
                            JOIN information_schema.key_column_usage ku
                                ON tc.constraint_name = ku.constraint_name
                            WHERE tc.constraint_type = 'PRIMARY KEY'
                            AND tc.table_schema = %s
                            AND tc.table_name = %s
                        ) pk ON c.column_name = pk.column_name
                        WHERE c.table_schema = %s AND c.table_name = %s
                    """, (schema, table, schema, table))
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
                                'primary_key': col[3]
                            } for col in columns
                        ]
                    }
        except Exception as e:
            raise OperationError(f"Failed to get table info: {str(e)}") from e

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

    def execute_query(self, query: str) -> List[Tuple]:
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    results = cursor.fetchall()
                    return "\n".join([str(row) for row in results])
        except Exception as e:
            return f"An error occurred: {str(e)}"
