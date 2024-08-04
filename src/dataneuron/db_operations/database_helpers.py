from typing import List, Optional, Any


class DatabaseHelper:
    def __init__(self, database_name: str, db: Any):
        self.database = database_name
        self.db = db

    def quote_identifier(self, identifier: str) -> str:
        if self.database == 'mysql':
            return f"`{identifier}`"
        elif self.database == 'mssql':
            return f"[{identifier}]"
        else:  # postgres and sqlite
            return f'"{identifier}"'

    def get_pattern_match_clause(self, column: str, value: str) -> str:
        if self.database == 'mssql':
            return f"LOWER({column}) LIKE '%' + LOWER('{value}') + '%'"
        else:  # postgres, mysql, and sqlite
            return f"LOWER({column}) LIKE LOWER('%{value}%')"

    def execute_query(self, query: str) -> List[tuple]:
        return self.db.execute_query(query)

    def top_few_records(self, column_name: str, table_name: str, potential_value: Optional[str] = None, limit: int = 10) -> str:
        quoted_table = table_name

        if column_name == '*':
            select_clause = '*'
            distinct_clause = ""
        else:
            quoted_column = self.quote_identifier(column_name)
            select_clause = quoted_column
            distinct_clause = "DISTINCT" if self.database == 'mssql' else "DISTINCT "

        where_clause = ""
        if potential_value:
            if column_name == '*':
                if self.database == 'mssql':
                    where_clause = f"WHERE EXISTS (SELECT 1 FROM {quoted_table} FOR JSON PATH) WHERE JSON_VALUE(BulkColumn, '$.*') LIKE '%{potential_value}%'"
                else:
                    where_clause = f"WHERE CAST({quoted_table} AS TEXT) LIKE '%{potential_value}%'"
            else:
                where_clause = f"WHERE {self.get_pattern_match_clause(quoted_column, potential_value)}"

        if self.database == 'mssql':
            query = f"""
            SELECT {distinct_clause} TOP {limit} {select_clause}
            FROM {quoted_table}
            {where_clause}
            """
        else:  # postgres, mysql, and sqlite
            query = f"""
            SELECT {distinct_clause}{select_clause}
            FROM {quoted_table}
            {where_clause}
            LIMIT {limit}
            """
        return query.strip()

    def get_sample_data(self, table_name: str, limit: int = 5) -> List[tuple]:
        query = self.top_few_records('*', table_name, limit=limit)
        return self.execute_query(query)
