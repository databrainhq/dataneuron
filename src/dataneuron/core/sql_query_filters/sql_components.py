import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Token, TokenList, Where, Parenthesis
from sqlparse.tokens import Keyword, DML
from typing import List, Dict, Optional
from .sql_parser import SQLParser, TableExtractor


class SQLParserImplementation(SQLParser):
    def parse(self, sql_query: str) -> TokenList:
        return sqlparse.parse(sql_query)[0]


class TableExtractorImplementation(TableExtractor):
    def extract_tables(self, parsed_query: TokenList) -> List[Dict[str, Optional[str]]]:
        tables = []
        self._extract_from_clause_tables(parsed_query, tables)
        self._extract_where_clause_tables(parsed_query, tables)
        return tables  # Make sure to return the extracted tables

    def _extract_from_clause_tables(self, parsed_query: TokenList, tables: List[Dict[str, Optional[str]]]):
        from_seen = False
        for token in parsed_query.tokens:
            if from_seen:
                if isinstance(token, Identifier):
                    if isinstance(token.tokens[0], Parenthesis):
                        # This is a subquery
                        subquery = token.tokens[0]
                        self._extract_from_clause_tables(subquery, tables)
                    else:
                        tables.append(self._parse_table_identifier(token))
                elif isinstance(token, IdentifierList):
                    for identifier in token.get_identifiers():
                        if isinstance(identifier, Identifier):
                            if isinstance(identifier.tokens[0], Parenthesis):
                                # This is a subquery
                                subquery = identifier.tokens[0]
                                self._extract_from_clause_tables(
                                    subquery, tables)
                            else:
                                tables.append(
                                    self._parse_table_identifier(identifier))
                elif token.ttype is Keyword and token.value.upper() in ('WHERE', 'GROUP', 'ORDER', 'LIMIT'):
                    break
            elif token.ttype is Keyword and token.value.upper() == 'FROM':
                from_seen = True
            elif token.ttype is Keyword and token.value.upper() == 'JOIN':
                tables.append(self._parse_table_identifier(
                    parsed_query.token_next(token)[1]))

    def _extract_where_clause_tables(self, parsed_query: TokenList, tables: List[Dict[str, Optional[str]]]):
        where_clause = next(
            (token for token in parsed_query.tokens if isinstance(token, Where)), None)
        if where_clause:
            for token in where_clause.tokens:
                if isinstance(token, Identifier):
                    if '.' in token.value:
                        schema, name = token.value.split('.', 1)
                        tables.append(
                            {'name': name, 'schema': schema, 'alias': None})

    def _parse_table_identifier(self, identifier: Identifier) -> Dict[str, Optional[str]]:
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

        return {'name': name, 'schema': schema, 'alias': alias}

    def _strip_quotes(self, identifier: str) -> str:
        return identifier.strip('"').strip("'").strip('`')
