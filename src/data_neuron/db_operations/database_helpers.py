from typing import List


class DatabaseHelper:
    def __init__(self, database_name: str, db):
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


def top_few_records(db_helper: DatabaseHelper, column_name: str, table_name: str, potential_value: str = None, limit: int = 10) -> str:
    # Handle the asterisk separately
    if column_name == '*':
        select_clause = '*'
        distinct_clause = ""
    else:
        quoted_column = db_helper.quote_identifier(column_name)
        select_clause = quoted_column
        distinct_clause = "DISTINCT" if db_helper.database == 'mssql' else "DISTINCT "

    where_clause = ""
    if potential_value:
        if column_name == '*':
            # When column is '*', we can't use it in the WHERE clause
            # Instead, we'll search across all columns
            where_clause = f"WHERE EXISTS (SELECT 1 FROM {table_name} FOR JSON PATH) WHERE JSON_VALUE(BulkColumn, '$.*') LIKE '%{potential_value}%'"
        else:
            where_clause = f"WHERE {db_helper.get_pattern_match_clause(quoted_column, potential_value)}"

    if db_helper.database == 'mssql':
        query = f"""
        SELECT {distinct_clause} TOP {limit}  {select_clause}
        FROM {table_name}
        {where_clause}
        """
    else:  # postgres, mysql, and sqlite
        query = f"""
        SELECT {distinct_clause}{select_clause}
        FROM {table_name}
        {where_clause}
        LIMIT {limit}
        """
    return query.strip()
