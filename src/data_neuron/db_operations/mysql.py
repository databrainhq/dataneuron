from .base import DatabaseOperations


class MySQLOperations(DatabaseOperations):
    def __init__(self, host: str, user: str, password: str, database: str):
        self.conn_params = {
            "host": host,
            "user": user,
            "password": password,
            "database": database
        }

    def execute_query(self, query: str) -> str:
        try:
            import mysql.connector
            with mysql.connector.connect(**self.conn_params) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    results = cursor.fetchall()
                    return "\n".join([str(row) for row in results])
        except ImportError:
            return "MySQL support is not installed. Please install it with 'pip install your_cli_tool[mysql]'"
        except Exception as e:
            return f"An error occurred: {str(e)}"

    def get_schema_info(self) -> str:
        try:
            import mysql.connector
            with mysql.connector.connect(**self.conn_params) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SHOW TABLES;")
                    tables = cursor.fetchall()
                    schema_info = []
                    for table in tables:
                        table_name = table[0]
                        cursor.execute(f"DESCRIBE {table_name};")
                        columns = cursor.fetchall()
                        schema_info.append(f"\nTable: {table_name}")
                        for column in columns:
                            schema_info.append(f"  {column[0]} ({column[1]})")
                    return "\n".join(schema_info)
        except ImportError:
            return "MySQL support is not installed. Please install it with 'pip install your_cli_tool[mysql]'"
        except Exception as e:
            return f"An error occurred: {str(e)}"
