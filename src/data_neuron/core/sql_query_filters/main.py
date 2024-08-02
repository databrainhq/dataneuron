import re
from sqlparse.sql import Token, Where, Parenthesis, TokenList, Identifier
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
        print(f"Parsed query: {parsed_query}")

        if self._contains_subquery(parsed_query):
            print("Subquery detected, handling subquery")
            return self._handle_subquery(parsed_query, client_id)
        else:
            print("No subquery detected, applying filter to single query")
            return self._apply_filter_to_single_query(parsed_query, client_id)

    def _contains_set_operation(self, parsed_query: TokenList) -> bool:
        set_operations = ('UNION', 'INTERSECT', 'EXCEPT')
        return any(token.ttype is Keyword and token.value.upper() in set_operations for token in parsed_query.tokens)

    def _handle_set_operation(self, parsed, client_id):
        statements = []
        current_statement = []
        for token in parsed.tokens:
            if token.ttype is Keyword and token.value.upper() in ('UNION', 'INTERSECT', 'EXCEPT'):
                if current_statement:
                    statements.append(TokenList(current_statement))
                    current_statement = []
                statements.append(token)
            else:
                current_statement.append(token)
        if current_statement:
            statements.append(TokenList(current_statement))

        filtered_statements = []
        for statement in statements:
            if isinstance(statement, TokenList):
                filtered_statement = self._apply_filter_recursive(
                    statement, client_id)
                filtered_statements.append(str(filtered_statement))
            else:
                filtered_statements.append(str(statement))

        return ' '.join(filtered_statements)

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

    def _handle_subquery(self, parsed, client_id):
        result = []
        for token in parsed.tokens:
            if isinstance(token, Identifier) and token.has_alias():
                if isinstance(token.tokens[0], Parenthesis):
                    subquery = token.tokens[0].tokens[1:-1]
                    filtered_subquery = self._apply_filter_recursive(
                        TokenList(subquery), client_id)
                    alias = token.get_alias()
                    result.append(f"({filtered_subquery}) AS {alias}")
                else:
                    result.append(str(token))
            elif isinstance(token, Parenthesis):
                subquery = token.tokens[1:-1]
                filtered_subquery = self._apply_filter_recursive(
                    TokenList(subquery), client_id)
                result.append(f"({filtered_subquery})")
            else:
                result.append(str(token))
        return ' '.join(result)

    def _apply_filter_to_single_query(self, parsed_query: TokenList, client_id: int) -> str:
        print(f"Applying filter to single query: {parsed_query}")
        tables = self.extractor.extract_tables(parsed_query)
        print(f"Extracted tables: {tables}")
        filters = []
        for table in tables:
            filter_condition = self.filter_applier.apply_filter(
                table, client_id)
            if filter_condition:
                filters.append(filter_condition)

        print(f"Generated filters: {filters}")

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

        result = str(parsed_query)
        print(f"Result after applying filter: {result}")
        return result
