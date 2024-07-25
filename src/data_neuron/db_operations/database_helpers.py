from typing import List


class DatabaseHelper:
    def __init__(self, database: str, db):
        self.database = database
        self.db = db

    def quote_identifier(self, identifier: str) -> str:
        if self.database == 'mysql':
            return f"`{identifier}`"
        elif self.database == 'mssql':
            return f"[{identifier}]"
        else:  # postgres and sqlite
            return f'"{identifier}"'

    def get_fully_qualified_table_name(self, schema_name: str, table_name: str) -> str:
        quoted_table = self.quote_identifier(table_name)
        if self.database == 'postgres':
            if schema_name and schema_name != 'public':
                return f'{self.quote_identifier(schema_name)}.{quoted_table}'
        elif self.database == 'mssql':
            if schema_name and schema_name != 'dbo':
                return f'{self.quote_identifier(schema_name)}.{quoted_table}'
        elif self.database == 'mysql':
            if schema_name:
                return f'{self.quote_identifier(schema_name)}.{quoted_table}'
        return quoted_table

    def get_pattern_match_clause(self, column: str, value: str) -> str:
        if self.database == 'mssql':
            return f"LOWER({column}) LIKE '%' + LOWER('{value}') + '%'"
        else:  # postgres, mysql, and sqlite
            return f"LOWER({column}) LIKE LOWER('%{value}%')"

    def execute_query(self, query: str) -> List[tuple]:
        return self.db.execute_query(query)


def top_few_records(db_helper: DatabaseHelper, column_name: str, schema_name: str, table_name: str, potential_value: str = None, limit: int = 10) -> str:
    quoted_table = db_helper.get_fully_qualified_table_name(
        schema_name, table_name)

    # Handle the asterisk separately
    if column_name == '*':
        select_clause = '*'
    else:
        quoted_column = db_helper.quote_identifier(column_name)
        select_clause = f"DISTINCT {quoted_column}"

    where_clause = ""
    if potential_value:
        where_clause = f"WHERE {db_helper.get_pattern_match_clause(quoted_column, potential_value)}"

    if db_helper.database == 'mssql':
        query = f"""
        SELECT TOP {limit} {select_clause}
        FROM {quoted_table}
        {where_clause}
        """
    else:  # postgres, mysql, and sqlite
        query = f"""
        SELECT {select_clause}
        FROM {quoted_table}
        {where_clause}
        LIMIT {limit}
        """

    return query.strip()