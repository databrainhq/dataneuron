from sqlparse.sql import Token, TokenList, Identifier, Parenthesis
from sqlparse.tokens import Keyword, DML
from typing import List
from .sql_parser import SubqueryHandler, ClientFilterApplier, SQLParser, SetOperationHandler


class SubqueryHandlerImplementation(SubqueryHandler):
    def __init__(self, parser: SQLParser, filter_applier: ClientFilterApplier):
        self.parser = parser
        self.filter_applier = filter_applier

    def handle_subquery(self, subquery: TokenList, client_id: int) -> str:
        print(f"Handling subquery: {subquery}")
        filtered_tokens = []
        for token in subquery.tokens:
            print(f"Processing token: {token}")
            if isinstance(token, Identifier) and token.has_alias():
                if isinstance(token.tokens[0], Parenthesis):
                    print("Found subquery with alias")
                    subquery_content = token.tokens[0].tokens[1:-1]
                    filtered_subquery = self.handle_subquery(
                        TokenList(subquery_content), client_id)
                    alias = f"AS {token.get_alias()}" if 'AS' in str(
                        token) else token.get_alias()
                    filtered_tokens.append(f"({filtered_subquery}) {alias}")
                else:
                    filtered_tokens.append(str(token))
            elif isinstance(token, Parenthesis):
                print("Found parenthesis")
                subquery_content = token.tokens[1:-1]
                filtered_subquery = self.handle_subquery(
                    TokenList(subquery_content), client_id)
                filtered_tokens.append(f"({filtered_subquery})")
            else:
                filtered_tokens.append(str(token))

        result = ' '.join(filtered_tokens)
        print(f"Subquery handling result: {result}")
        return result


class SetOperationHandlerImplementation(SetOperationHandler):
    def __init__(self, parser: SQLParser, filter_applier: ClientFilterApplier):
        self.parser = parser
        self.filter_applier = filter_applier

    def handle_set_operation(self, queries: List[str], operation: str, client_id: int) -> str:
        filtered_queries = []
        for query in queries:
            parsed_query = self.parser.parse(query)
            tables = self.filter_applier.extract_tables(parsed_query)
            filters = [self.filter_applier.apply_filter(
                table, client_id) for table in tables]
            filtered_query = self._inject_filters(query, filters)
            filtered_queries.append(filtered_query)

        return f" {operation} ".join(filtered_queries)

    def _inject_filters(self, query: str, filters: List[str]) -> str:
        if not filters:
            return query

        parsed = self.parser.parse(query)
        where_index = next((i for i, token in enumerate(parsed.tokens)
                            if token.ttype is Keyword and token.value.upper() == 'WHERE'), None)

        if where_index is not None:
            where_token = parsed.tokens[where_index]
            new_where = f"{where_token} AND {' AND '.join(filters)}"
            parsed.tokens[where_index] = Token(Keyword, new_where)
        else:
            parsed.tokens.append(Token(Keyword, 'WHERE'))
            parsed.tokens.append(Token(Keyword, ' AND '.join(filters)))

        return str(parsed)
