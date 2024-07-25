import sqlite3


class SQLiteOperations:
    def __init__(self, db_path):
        self.db_path = db_path

    def get_table_list(self):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table';")
            return [table[0] for table in cursor.fetchall()]

    def get_table_info(self, table_name):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            return {
                'table_name': table_name,
                'columns': [
                    {
                        'name': col[1],
                        'type': col[2],
                        'primary_key': col[5] == 1,
                        'nullable': col[3] == 0
                    } for col in columns
                ]
            }

    def execute_query_with_column_names(self, query):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            column_names = [description[0]
                            for description in cursor.description]
            results = cursor.fetchall()
            return results, column_names

    def execute_query(self, query):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            return results

    def get_schema_info(self) -> str:
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table';")
                tables = cursor.fetchall()
                schema_info = []
                for table in tables:
                    table_name = table[0]
                    cursor.execute(f"PRAGMA table_info({table_name});")
                    columns = cursor.fetchall()
                    schema_info.append(f"Table: {table_name}")
                    for column in columns:
                        schema_info.append(f"  {column[1]} ({column[2]})")
                return "\n".join(schema_info)
        except sqlite3.Error as e:
            return f"An error occurred: {str(e)}"
        except Exception as e:
            return f"An unexpected error occurred: {str(e)}"
