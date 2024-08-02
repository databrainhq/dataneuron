import re
from sqlparse.sql import Token, Where, Parenthesis, TokenList
from sqlparse.tokens import Keyword, DML
from typing import Optional, List, Dict
from .sql_parser import SQLParser, TableExtractor, ClientFilterApplier, SubqueryHandler, SetOperationHandler, WhereClauseModifier


class SQLQueryFilter:
    def __init__(self,
                 parser: SQLParser,
                 extractor: TableExtractor,
                 filter_applier: ClientFilterApplier,
                 subquery_handler: SubqueryHandler,
                 set_operation_handler: SetOperationHandler,
                 where_modifier: WhereClauseModifier):
        self.parser = parser
        self.extractor = extractor
        self.filter_applier = filter_applier
        self.subquery_handler = subquery_handler
        self.set_operation_handler = set_operation_handler
        self.where_modifier = where_modifier

    def apply_client_filter(self, sql_query: str, client_id: int) -> str:
        parsed_query = self.parser.parse(sql_query)

        if self._contains_set_operation(parsed_query):
            return self._handle_set_operation(sql_query, client_id)
        elif self._contains_subquery(parsed_query):
            return self._handle_subquery(parsed_query, client_id)
        else:
            return self._apply_filter_to_single_query(parsed_query, client_id)

    def _contains_set_operation(self, parsed_query: TokenList) -> bool:
        set_operations = ('UNION', 'INTERSECT', 'EXCEPT')
        return any(token.ttype is Keyword and token.value.upper() in set_operations for token in parsed_query.tokens)

    def _handle_set_operation(self, sql_query: str, client_id: int) -> str:
        queries = self._split_set_operation(sql_query)
        operation = self._get_set_operation(sql_query)
        filtered_queries = [self._apply_filter_to_single_query(
            self.parser.parse(q), client_id) for q in queries]
        return f" {operation} ".join(filtered_queries)

    def _split_set_operation(self, sql_query: str) -> List[str]:
        import re
        pattern = r'(?:^|\s)(UNION|INTERSECT|EXCEPT)(?:\s+ALL)?(?:\s|$)'
        parts = re.split(pattern, sql_query, flags=re.IGNORECASE)
        return [parts[0]] + [part.strip() for part in parts[2::2]]

    def _get_set_operation(self, sql_query: str) -> str:
        import re
        match = re.search(r'(UNION|INTERSECT|EXCEPT)(\s+ALL)?',
                          sql_query, re.IGNORECASE)
        return match.group().upper() if match else ""

    def _contains_subquery(self, parsed_query: TokenList) -> bool:
        for token in parsed_query.tokens:
            if isinstance(token, TokenList):
                if any(t.ttype is DML and t.value.upper() == 'SELECT' for t in token.tokens):
                    return True
        return False

    def _handle_subquery(self, parsed_query: TokenList, client_id: int) -> str:
        return self.subquery_handler.handle_subquery(parsed_query, client_id)

    def _apply_filter_to_single_query(self, parsed_query: TokenList, client_id: int) -> str:
        tables = self.extractor.extract_tables(parsed_query)
        filters = []
        for table in tables:
            filter_condition = self.filter_applier.apply_filter(
                table, client_id)
            if filter_condition:
                filters.append(filter_condition)

        if not filters:
            return str(parsed_query)

        where_clause = next(
            (token for token in parsed_query.tokens if isinstance(token, Where)), None)
        new_where_clause = self.where_modifier.modify_where_clause(
            where_clause, ' AND '.join(filters))

        if where_clause:
            where_index = parsed_query.token_index(where_clause)
            parsed_query.tokens[where_index] = new_where_clause
        else:
            parsed_query.tokens.append(Token(Keyword, ' '))
            parsed_query.tokens.append(new_where_clause)

        return str(parsed_query)
