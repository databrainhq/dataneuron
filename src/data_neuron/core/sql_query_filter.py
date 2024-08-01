import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Token, TokenList
from sqlparse.tokens import Keyword, DML, Name, Whitespace, Punctuation
from typing import List, Dict, Optional


class SQLQueryFilter:
    def __init__(self, client_tables: Dict[str, str], schemas: List[str] = ['main'], case_sensitive: bool = False):
        self.client_tables = client_tables
        self.schemas = schemas
        self.case_sensitive = case_sensitive

    def apply_client_filter(self, sql_query: str, client_id: int) -> str:
        # Parse the entire SQL query
        parsed = sqlparse.parse(sql_query)[0]
        print(f"Parsed query: {parsed}")
        print(
            f"Tokens: {[(str(token), token.ttype) for token in parsed.tokens]}")

        # Check if the query contains UNION, INTERSECT, or EXCEPT
        if self._contains_set_operation(parsed):
            print("Set operation detected, handling set operation")
            return self._handle_set_operation(parsed, client_id)
        else:
            print("No set operation detected, applying filter to single query")
            return self._apply_filter_to_single_query(sql_query, client_id)

    def _contains_set_operation(self, parsed):
        set_operations = ('UNION', 'INTERSECT', 'EXCEPT')
        for i, token in enumerate(parsed.tokens):
            if token.ttype is Keyword:
                # Check for 'UNION ALL' as a single token
                if token.value.upper() == 'UNION ALL':
                    print("Set operation found: UNION ALL")
                    return True
                # Check for 'UNION', 'INTERSECT', 'EXCEPT' followed by 'ALL'
                if token.value.upper() in set_operations:
                    next_token = parsed.token_next(i)
                    if next_token and next_token[1].value.upper() == 'ALL':
                        print(f"Set operation found: {token.value} ALL")
                        return True
                    else:
                        print(f"Set operation found: {token.value}")
                        return True
        print("No set operation found")
        return False

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
            elif token.ttype is Keyword and token.value.upper() == 'JOIN':
                tables_info.append(self._parse_table_identifier(
                    parsed.token_next(token)[1]))

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
            name = f"{schema}.{name}" if schema else name

        return {'name': name, 'schema': schema, 'alias': alias}

    def _find_matching_table(self, table_name: str, schema: Optional[str] = None) -> Optional[str]:
        possible_names = [
            f"{schema}.{table_name}" if schema else table_name,
            table_name,
        ] + [f"{s}.{table_name}" for s in self.schemas]

        for name in possible_names:
            if self._case_insensitive_get(self.client_tables, name) is not None:
                return name
        return None

    def _case_insensitive_get(self, dict_obj: Dict[str, str], key: str) -> Optional[str]:
        if self.case_sensitive:
            return dict_obj.get(key)
        return next((v for k, v in dict_obj.items() if k.lower() == key.lower()), None)

    def _strip_quotes(self, identifier: str) -> str:
        return identifier.strip('"').strip("'").strip('`')

    def _quote_identifier(self, identifier: str) -> str:
        return f'"{identifier}"'

    def _inject_where_clause(self, parsed, where_clause):
        print(f"Parsed tokens: {[str(token) for token in parsed.tokens]}")
        where_index = next((i for i, token in enumerate(parsed.tokens)
                            if token.ttype is Keyword and token.value.upper() == 'WHERE'), None)
        print(f"WHERE index: {where_index}")

        if where_index is not None:
            print("Existing WHERE clause found")
            # WHERE clause exists, find the end of the existing WHERE clause
            end_where_index = len(parsed.tokens) - 1
            for i in range(where_index + 1, len(parsed.tokens)):
                token = parsed.tokens[i]
                if token.ttype is Keyword and token.value.upper() in ('GROUP', 'ORDER', 'LIMIT'):
                    end_where_index = i - 1
                    break
            print(f"End of WHERE clause index: {end_where_index}")

            # Insert our condition at the end of the existing WHERE clause
            parsed.tokens.insert(end_where_index + 1, Token(Whitespace, ' '))
            parsed.tokens.insert(end_where_index + 2, Token(Keyword, 'AND'))
            parsed.tokens.insert(end_where_index + 3, Token(Whitespace, ' '))
            parsed.tokens.insert(end_where_index + 4,
                                 Token(Name, where_clause))
        else:
            print("No existing WHERE clause found")
            # No WHERE clause, add one
            parsed.tokens.append(Token(Whitespace, ' '))
            parsed.tokens.append(Token(Keyword, 'WHERE'))
            parsed.tokens.append(Token(Whitespace, ' '))
            parsed.tokens.append(Token(Name, where_clause))

        print(
            f"Final parsed tokens: {[str(token) for token in parsed.tokens]}")
        return str(parsed)

    def _handle_set_operation(self, parsed, client_id):
        print("Handling set operation")
        # Split the query into individual SELECT statements
        statements = []
        current_statement = []
        set_operation = None
        for token in parsed.tokens:
            if token.ttype is Keyword and token.value.upper() in ('UNION', 'INTERSECT', 'EXCEPT', 'UNION ALL'):
                if current_statement:
                    statements.append(''.join(str(t)
                                      for t in current_statement).strip())
                    current_statement = []
                set_operation = token.value.upper()
            else:
                current_statement.append(token)

        if current_statement:
            statements.append(''.join(str(t)
                              for t in current_statement).strip())

        print(f"Split statements: {statements}")
        print(f"Set operation: {set_operation}")

        # Apply the filter to each SELECT statement
        filtered_statements = []
        for stmt in statements:
            filtered_stmt = self._apply_filter_to_single_query(stmt, client_id)
            filtered_statements.append(filtered_stmt)
            print(f"Filtered statement: {filtered_stmt}")

        # Reconstruct the query
        result = f" {set_operation} ".join(filtered_statements)
        print(f"Final result: {result}")
        return result

    def _apply_filter_to_single_query(self, sql_query: str, client_id: int) -> str:
        parts = sql_query.split(' WHERE ', 1)

        parsed = sqlparse.parse(parts[0])[0]
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
            new_condition = " AND ".join(filters)
            if len(parts) > 1:
                # There was an existing WHERE clause
                where_parts = parts[1].split(' GROUP BY ', 1)
                existing_where = where_parts[0].strip()
                group_by = f" GROUP BY {where_parts[1]}" if len(
                    where_parts) > 1 else ""

                # Check if the existing WHERE clause contains OR conditions
                if ' OR ' in existing_where.upper():
                    # If it contains OR, wrap it in parentheses if not already
                    if not (existing_where.startswith('(') and existing_where.endswith(')')):
                        existing_where = f"({existing_where})"

                return f"{parts[0]} WHERE {existing_where} AND {new_condition}{group_by}"
            else:
                # There was no existing WHERE clause
                return f"{sql_query} WHERE {new_condition}"
        else:
            return sql_query
