import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Token, TokenList
from sqlparse.tokens import Keyword, DML, Name, Whitespace
from typing import List, Dict, Optional


class SQLQueryFilter:
    def __init__(self, client_tables: Dict[str, str], schemas: List[str] = ['main'], case_sensitive: bool = False):
        self.client_tables = client_tables
        self.schemas = schemas
        self.case_sensitive = case_sensitive

    def apply_client_filter(self, sql_query: str, client_id: int) -> str:
        parsed = sqlparse.parse(sql_query)[0]
        tables_info = self._extract_tables_info(parsed)

        filters = []
        for table_info in tables_info:
            table_name = table_info['name']
            table_alias = table_info['alias']
            schema = table_info['schema']

            matching_table = self._find_matching_table(table_name, schema)

            if matching_table:
                client_id_column = self.client_tables[matching_table]
                table_reference = table_alias or table_name
                filters.append(
                    f'{self._quote_identifier(table_reference)}.{self._quote_identifier(client_id_column)} = {client_id}')

        if filters:
            where_clause = " AND ".join(filters)
            return self._inject_where_clause(parsed, where_clause)
        else:
            return sql_query

    def _extract_tables_info(self, parsed):
        tables_info = []
        self._extract_from_clause_tables(parsed, tables_info)
        return tables_info

    def _extract_from_clause_tables(self, parsed, tables_info):
        from_seen = False
        for token in parsed.tokens:
            if from_seen:
                if isinstance(token, Identifier):
                    tables_info.append(self._parse_table_identifier(token))
                elif isinstance(token, IdentifierList):
                    for identifier in token.get_identifiers():
                        if isinstance(identifier, Identifier):
                            tables_info.append(
                                self._parse_table_identifier(identifier))
                elif token.ttype is Keyword and token.value.upper() in ('WHERE', 'GROUP', 'ORDER', 'LIMIT'):
                    break
            elif token.ttype is Keyword and token.value.upper() == 'FROM':
                from_seen = True

    def _find_matching_table(self, table_name: str, schema: Optional[str] = None) -> Optional[str]:
        possible_names = [
            f"{schema}.{table_name}" if schema else table_name,
            table_name,
        ] + [f"{s}.{table_name}" for s in self.schemas]

        for name in possible_names:
            if self._case_insensitive_get(self.client_tables, name) is not None:
                return name
        return None

    def _parse_table_identifier(self, identifier):
        schema = None
        alias = None
        name = self._strip_quotes(str(identifier))

        if identifier.has_alias():
            alias = self._strip_quotes(identifier.get_alias())
            name = self._strip_quotes(identifier.get_real_name())

        if '.' in name:
            parts = name.split('.')
            if len(parts) == 2:
                schema, name = parts
            # Keep the full name for schema-qualified tables
            name = f"{schema}.{name}"

        return {'name': name, 'schema': schema, 'alias': alias}

    def _case_insensitive_get(self, dict_obj: Dict[str, str], key: str) -> Optional[str]:
        if self.case_sensitive:
            return dict_obj.get(key)
        return next((v for k, v in dict_obj.items() if k.lower() == key.lower()), None)

    def _strip_quotes(self, identifier: str) -> str:
        return identifier.strip('"').strip("'").strip('`')

    def _quote_identifier(self, identifier: str) -> str:
        return f'"{identifier}"'

    def _inject_where_clause(self, parsed, where_clause):
        where_index = next((i for i, token in enumerate(parsed.tokens)
                            if token.ttype is Keyword and token.value.upper() == 'WHERE'), None)

        if where_index is not None:
            # WHERE clause exists, inject our condition
            parsed.tokens.insert(where_index + 1, Token(Whitespace, ' '))
            parsed.tokens.insert(where_index + 2, Token(Name, where_clause))
            parsed.tokens.insert(where_index + 3, Token(Whitespace, ' '))
            parsed.tokens.insert(where_index + 4, Token(Keyword, 'AND'))
        else:
            # No WHERE clause, add one
            parsed.tokens.append(Token(Whitespace, ' '))
            parsed.tokens.append(Token(Keyword, 'WHERE'))
            parsed.tokens.append(Token(Whitespace, ' '))
            parsed.tokens.append(Token(Name, where_clause))

        return str(parsed)
