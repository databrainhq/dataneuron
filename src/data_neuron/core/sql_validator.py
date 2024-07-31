import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Function
from sqlparse.tokens import Keyword, DML
import re


class SQLQueryValidator:
    def __init__(self, context):
        self.context = context
        self.allowed_tables = set(context['tables'].keys())
        self.table_aliases = {alias: table for table, info in context['tables'].items()
                              for alias in [info.get('alias', ''), table]}
        self.global_aliases = {alias: table for alias, table in context['global_definitions'].get(
            'global_aliases', {}).get('table_aliases', {}).items()}
        self.table_aliases.update(self.global_aliases)
        print(f"Allowed tables: {self.allowed_tables}")
        print(f"Table aliases: {self.table_aliases}")

    def validate_and_sanitize(self, query):
        print(f"Validating query: {query}")
        parsed = sqlparse.parse(query)[0]

        # Check if it's a SELECT statement
        if not parsed.get_type() == 'SELECT':
            raise ValueError("Only SELECT statements are allowed.")

        # Validate tables
        used_tables = self._extract_table_names(parsed)
        print(f"Extracted table names: {used_tables}")
        for table in used_tables:
            if table not in self.allowed_tables and table not in self.table_aliases:
                raise ValueError(
                    f"Table '{table}' is not allowed or doesn't exist in the context.")

        # Add LIMIT if not present
        if not self._has_limit(parsed):
            query = self._add_limit(query)

        return query

    def _extract_table_names(self, parsed):
        tables = set()
        for token in parsed.tokens:
            if isinstance(token, IdentifierList):
                for identifier in token.get_identifiers():
                    if identifier.has_alias():
                        tables.add(identifier.get_real_name())
                    else:
                        tables.add(identifier.get_name())
            elif isinstance(token, Identifier):
                if token.has_alias():
                    tables.add(identifier.get_real_name())
                else:
                    tables.add(token.get_name())
            elif token.ttype is Keyword and token.value.upper() == 'FROM':
                tables.update(self._extract_from_clause_tables(token))
        return {self.table_aliases.get(table, table) for table in tables}

    def _extract_from_clause_tables(self, from_token):
        tables = set()
        for token in from_token.parent.tokens[from_token.parent.token_index(from_token):]:
            if isinstance(token, IdentifierList):
                for identifier in token.get_identifiers():
                    if identifier.has_alias():
                        tables.add(identifier.get_real_name())
                    else:
                        tables.add(identifier.get_name())
            elif isinstance(token, Identifier):
                if token.has_alias():
                    tables.add(token.get_real_name())
                else:
                    tables.add(token.get_name())
            elif token.ttype is Keyword and token.value.upper() in ('WHERE', 'GROUP', 'ORDER', 'LIMIT'):
                break
        return tables

    def _has_limit(self, parsed):
        return any(token.ttype is Keyword and token.value.upper() == 'LIMIT' for token in parsed.tokens)

    def _add_limit(self, query):
        return f"{query.rstrip(';')} LIMIT 1000;"


def sanitize_sql_query(query, context):
    validator = SQLQueryValidator(context)
    return validator.validate_and_sanitize(query)
