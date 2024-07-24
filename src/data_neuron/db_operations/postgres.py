import psycopg2
from .base import DatabaseOperations


class PostgreSQLOperations(DatabaseOperations):
    def __init__(self, dbname: str, user: str, password: str, host: str, port: str):
        self.conn_params = {
            "dbname": dbname,
            "user": user,
            "password": password,
            "host": host,
            "port": port
        }

    def execute_query(self, query: str) -> str:
        try:
            import psycopg2
            with psycopg2.connect(**self.conn_params) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    results = cursor.fetchall()
                    return "\n".join([str(row) for row in results])
        except ImportError:
            return "PostgreSQL support is not installed. Please install it with 'pip install your_cli_tool[postgres]'"
        except Exception as e:
            return f"An error occurred: {str(e)}"

    def get_schema_info(self) -> str:
        try:
            import psycopg2
            with psycopg2.connect(**self.conn_params) as conn:
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
        except ImportError:
            return "PostgreSQL support is not installed. Please install it with 'pip install your_cli_tool[postgres]'"
        except Exception as e:
            return f"An error occurred: {str(e)}"
