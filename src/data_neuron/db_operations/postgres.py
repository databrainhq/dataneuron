from .base import DatabaseOperations
from .exceptions import ConnectionError, OperationError


class PostgreSQLOperations(DatabaseOperations):
    def __init__(self, dbname: str, user: str, password: str, host: str, port: str):
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

    def get_table_list(self):
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
                    return [table[0] for table in cursor.fetchall()]
        except Exception as e:
            raise OperationError(f"Failed to get table list: {str(e)}") from e

    def get_table_info(self, table_name):
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
                            AND tc.table_name = %s
                        ) pk ON c.column_name = pk.column_name
                        WHERE c.table_name = %s
                    """, (table_name, table_name))
                    columns = cursor.fetchall()
                    return {
                        'table_name': table_name,
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
            return f"An error occurred: {str(e)}"

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
                    cursor.execute("""
                        SELECT table_name, column_name, data_type
                        FROM information_schema.columns
                        WHERE table_schema = 'public'
                        ORDER BY table_name, ordinal_position;
                    """)
                    results = cursor.fetchall()
                    schema_info = []
                    current_table = ""
                    for row in results:
                        if row[0] != current_table:
                            current_table = row[0]
                            schema_info.append(f"\nTable: {current_table}")
                        schema_info.append(f"  {row[1]} ({row[2]})")
                    return "\n".join(schema_info)
        except Exception as e:
            return f"An error occurred: {str(e)}"
