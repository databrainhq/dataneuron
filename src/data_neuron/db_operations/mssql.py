from .base import DatabaseOperations


class MSSQLOperations(DatabaseOperations):
    def __init__(self, server: str, database: str, username: str, password: str):
        self.conn_params = {
            "server": server,
            "database": database,
            "username": username,
            "password": password
        }

    def execute_query(self, query: str) -> str:
        try:
            import pyodbc
            conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.conn_params['server']};DATABASE={self.conn_params['database']};UID={self.conn_params['username']};PWD={self.conn_params['password']}"
            with pyodbc.connect(conn_str) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    results = cursor.fetchall()
                    return "\n".join([str(row) for row in results])
        except ImportError:
            return "MSSQL support is not installed. Please install it with 'pip install your_cli_tool[mssql]'"
        except Exception as e:
            return f"An error occurred: {str(e)}"

    def get_schema_info(self) -> str:
        try:
            import pyodbc
            conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.conn_params['server']};DATABASE={self.conn_params['database']};UID={self.conn_params['username']};PWD={self.conn_params['password']}"
            with pyodbc.connect(conn_str) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS")
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
            return "MSSQL support is not installed. Please install it with 'pip install your_cli_tool[mssql]'"
        except Exception as e:
            return f"An error occurred: {str(e)}"
