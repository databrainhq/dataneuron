import re
import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Token, TokenList, Parenthesis, Where, Comparison
from sqlparse.tokens import Keyword, DML, Name, Whitespace, Punctuation
from typing import List, Dict, Optional
import logging
from .nlp_helpers.cte_handler import handle_cte_query
from .nlp_helpers.is_cte import is_cte_query

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class SQLQueryFilter:
    def __init__(self, client_tables: Dict[str, str], schemas: List[str] = ['main'], case_sensitive: bool = False):
        self.client_tables = client_tables
        self.schemas = schemas
        self.case_sensitive = case_sensitive
        self.filtered_tables = set()
        self._is_cte_query = is_cte_query

    def apply_client_filter(self, sql_query: str, client_id: int) -> str:
        self.filtered_tables = set()
        parsed = sqlparse.parse(sql_query)[0]

        logger.debug(f"Original query: {sql_query}")
        logger.debug(
            f"Parsed query tokens: {[str(token) for token in parsed.tokens]}")

        is_cte = self._is_cte_query(parsed)
        logger.debug(f"Is CTE query: {is_cte}")

        if is_cte:
            logger.debug(f"CTE query detected: {parsed}")
            return handle_cte_query(parsed, self._apply_filter_recursive, client_id)
        else:
            result = self._apply_filter_recursive(parsed, client_id)

        logger.debug(f"Final result after filtering: {result}")

        return self._cleanup_whitespace(str(result))

    def _apply_filter_recursive(self, parsed, client_id):
        if self._is_cte_query(parsed):
            logger.debug(f"Applying filter to CTE recursively: {parsed}")
            return handle_cte_query(parsed, self._apply_filter_recursive, client_id)

        logger.debug(f"Applying filter recursively to: {parsed}")
        if isinstance(parsed, Token) and parsed.ttype is DML:
            return self._apply_filter_to_single_query(str(parsed), client_id)
        elif self._contains_set_operation(parsed):
            return self._handle_set_operation(parsed, client_id)
        elif self._contains_subquery(parsed):
            return self._handle_subquery(parsed, client_id)
        else:
            filtered_query = self._apply_filter_to_single_query(
                str(parsed), client_id)
            return self._handle_where_subqueries(sqlparse.parse(filtered_query)[0], client_id)

    def _contains_set_operation(self, parsed):
        set_operations = ('UNION', 'INTERSECT', 'EXCEPT')

        # Check if parsed is a TokenList (has tokens attribute)
        if hasattr(parsed, 'tokens'):
            tokens = parsed.tokens
        else:
            # If it's a single Token, wrap it in a list
            tokens = [parsed]

        for i, token in enumerate(tokens):
            if token.ttype is Keyword:
                # Check for 'UNION ALL' as a single token
                if token.value.upper() == 'UNION ALL':
                    print("Set operation found: UNION ALL")
                    return True
                # Check for 'UNION', 'INTERSECT', 'EXCEPT' followed by 'ALL'
                if token.value.upper() in set_operations:
                    next_token = parsed.token_next(i) if hasattr(
                        parsed, 'token_next') else None
                    if next_token and next_token[1].value.upper() == 'ALL':
                        print(f"Set operation found: {token.value} ALL")
                        return True
                    else:
                        print(f"Set operation found: {token.value}")
                        return True
        print("No set operation found")
        return False

    def _extract_tables_info(self, parsed):
        logger.debug(f"Extracting tables info from: {parsed}")
        tables_info = []
        self._extract_from_clause_tables(parsed, tables_info)
        self._extract_where_clause_tables(parsed, tables_info)
        return tables_info

    def _extract_where_clause_tables(self, parsed, tables_info):
        logger.debug("Extracting tables from WHERE clause")
        where_clause = next(
            (token for token in parsed.tokens if isinstance(token, Where)), None)
        if where_clause:
            for token in where_clause.tokens:
                if isinstance(token, Comparison):
                    for item in token.tokens:
                        if isinstance(item, Identifier):
                            if '.' in item.value:
                                schema, name = item.value.split('.', 1)
                                tables_info.append(
                                    {'name': name, 'schema': schema, 'alias': None})
                        elif isinstance(item, Parenthesis):
                            subquery = ' '.join(str(t)
                                                for t in item.tokens[1:-1])
                            subquery_parsed = sqlparse.parse(subquery)[0]
                            self._extract_from_clause_tables(
                                subquery_parsed, tables_info)

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
        logger.info(f"Applying filter to single query: {sql_query}")

        parts = sql_query.split(' GROUP BY ')
        main_query = parts[0]
        group_by = f" GROUP BY {parts[1]}" if len(parts) > 1 else ""

        parsed = sqlparse.parse(main_query)[0]
        tables_info = self._extract_tables_info(parsed)
        logger.debug(f"Extracted tables info: {tables_info}")

        filters = []
        for table_info in tables_info:
            table_name = table_info['name']
            table_alias = table_info['alias']
            schema = table_info['schema']

            matching_table = self._find_matching_table(table_name, schema)
            logger.debug(f"Matching table for {table_name}: {matching_table}")

            if matching_table and matching_table not in self.filtered_tables:
                client_id_column = self.client_tables[matching_table]
                table_reference = table_alias or table_name
                filters.append(
                    f'{self._quote_identifier(table_reference)}.{self._quote_identifier(client_id_column)} = {client_id}')
                self.filtered_tables.add(matching_table)

        logger.debug(f"Generated filters: {filters}")

        if filters:
            where_clause = " AND ".join(filters)
            if 'WHERE' in main_query.upper():
                where_parts = main_query.split('WHERE', 1)
                result = f"{where_parts[0]} WHERE {where_parts[1].strip()} AND {where_clause}"
            else:
                result = f"{main_query} WHERE {where_clause}"
        else:
            result = main_query

        return result + group_by

    def _contains_subquery(self, parsed):
        logger.debug(f"Checking for subqueries in: {parsed}")
        tokens = parsed.tokens if hasattr(parsed, 'tokens') else [parsed]

        for i, token in enumerate(tokens):
            if isinstance(token, Identifier) and token.has_alias():
                if isinstance(token.tokens[0], Parenthesis):
                    logger.debug(f"Found subquery in identifier: {token}")
                    return True
            elif isinstance(token, Parenthesis):
                logger.debug(f"Examining parenthesis content: {token.tokens}")
                if any(t.ttype is DML and t.value.upper() == 'SELECT' for t in token.tokens):
                    logger.debug(f"Found SELECT in parenthesis: {token}")
                    return True
                # Recursively check inside parentheses
                if self._contains_subquery(token):
                    return True
            elif isinstance(token, Where):
                logger.debug("Examining WHERE clause")
                in_found = False
                for j, sub_token in enumerate(token.tokens):
                    if in_found:
                        if isinstance(sub_token, Parenthesis):
                            if any(t.ttype is DML and t.value.upper() == 'SELECT' for t in sub_token.tokens):
                                return True
                        elif hasattr(sub_token, 'ttype') and not sub_token.is_whitespace:
                            # Check if the token is a parenthesis-like structure
                            if '(' in sub_token.value and ')' in sub_token.value:
                                logger.debug(
                                    f"Found parenthesis-like structure: {sub_token.value}")
                                if 'SELECT' in sub_token.value.upper():
                                    logger.debug(
                                        f"Found SELECT in parenthesis-like structure: {sub_token.value}")
                                    return True
                            # If we find a non-whitespace token that's not a parenthesis, reset in_found
                            in_found = False
                    elif hasattr(sub_token, 'ttype') and sub_token.ttype is Keyword and sub_token.value.upper() == 'IN':
                        logger.debug("Found IN keyword")
                        in_found = True
                    elif isinstance(sub_token, Comparison):
                        logger.debug(f"Examining comparison: {sub_token}")
                        for item in sub_token.tokens:
                            if isinstance(item, Parenthesis):
                                logger.debug(
                                    f"Examining parenthesis in comparison: {item}")
                                if self._contains_subquery(item):
                                    return True
            elif hasattr(token, 'ttype') and token.ttype is Keyword and token.value.upper() == 'IN':
                logger.debug("Found IN keyword outside WHERE")
                next_token = tokens[i+1] if i+1 < len(tokens) else None
                if next_token:
                    if isinstance(next_token, Parenthesis):
                        logger.debug(f"Examining IN clause: {next_token}")
                        if any(t.ttype is DML and t.value.upper() == 'SELECT' for t in next_token.tokens):
                            logger.debug(
                                f"Found subquery in IN clause: {next_token}")
                            return True
                    elif hasattr(next_token, 'value') and '(' in next_token.value and ')' in next_token.value:
                        logger.debug(
                            f"Found parenthesis-like structure after IN: {next_token.value}")
                        if 'SELECT' in next_token.value.upper():
                            logger.debug(
                                f"Found SELECT in parenthesis-like structure after IN: {next_token.value}")
                            return True

        logger.debug("No subqueries found")
        return False

    def _cleanup_whitespace(self, query: str) -> str:
        # Remove extra spaces
        query = ' '.join(query.split())
        # Ensure single space after commas
        query = re.sub(r'\s*,\s*', ', ', query)
        return query

    def _handle_subquery(self, parsed, client_id):
        logger.info(f"Handling subquery: {parsed}")
        result = []
        tokens = parsed.tokens if hasattr(parsed, 'tokens') else [parsed]

        for token in tokens:
            if isinstance(token, Identifier) and token.has_alias():
                if isinstance(token.tokens[0], Parenthesis):
                    logger.debug("Found subquery in parenthesis")
                    subquery = token.tokens[0].tokens[1:-1]
                    subquery_str = ' '.join(str(t) for t in subquery)
                    filtered_subquery = self._apply_filter_recursive(
                        sqlparse.parse(subquery_str)[0], client_id)
                    alias = f"AS {token.get_alias()}" if 'AS' in str(
                        token) else token.get_alias()
                    result.append(f"({filtered_subquery}) {alias}")
                else:
                    result.append(str(token))
            elif isinstance(token, Parenthesis):
                logger.debug("Found non-aliased subquery in parenthesis")
                subquery = token.tokens[1:-1]
                subquery_str = ' '.join(str(t) for t in subquery)
                filtered_subquery = self._apply_filter_recursive(
                    sqlparse.parse(subquery_str)[0], client_id)
                result.append(f"({filtered_subquery})")
            elif isinstance(token, Where):
                logger.debug("Found WHERE clause, handling subqueries")
                try:
                    filtered_where = self._handle_where_subqueries(
                        token, client_id)
                    result.append(str(filtered_where))
                except Exception as e:
                    logger.error(f"Error handling WHERE clause: {e}")
                    # Keep original if handling fails
                    result.append(str(token))
            else:
                result.append(str(token))

        final_result = ' '.join(result)
        logger.info(f"Subquery result: {final_result}")
        return final_result

    def _handle_where_subqueries(self, where_clause, client_id):
        logger.info(f"Handling WHERE clause subqueries in: {where_clause}")
        if self._is_cte_query(where_clause):
            logger.debug(f"Handling WHERE clause for CTE: {where_clause}")
            cte_part = self._extract_cte_definition(where_clause)
            main_query = self._extract_main_query(where_clause)

            filtered_cte = self._apply_filter_recursive(cte_part, client_id)

            if 'WHERE' not in str(main_query).upper():
                logger.debug("Adding WHERE clause to main query in CTE")
                main_query = self._add_where_clause_to_main_query(
                    main_query, client_id)

            return f"{filtered_cte} {main_query}"
        else:
            new_where_tokens = []
            i = 0
            while i < len(where_clause.tokens):
                token = where_clause.tokens[i]
                if token.ttype is Keyword and token.value.upper() == 'IN':
                    logger.debug("Found 'IN' keyword")
                    next_token = where_clause.token_next(i)
                    if next_token and isinstance(next_token[1], Parenthesis):
                        logger.debug("Found parenthesis after 'IN'")
                        subquery = next_token[1].tokens[1:-1]
                        subquery_str = ' '.join(str(t) for t in subquery)
                        logger.debug(f"Subquery: {subquery_str}")
                        filtered_subquery = self._apply_filter_recursive(
                            sqlparse.parse(subquery_str)[0], client_id)
                        filtered_subquery_str = str(filtered_subquery)
                        logger.debug(
                            f"Filtered subquery: {filtered_subquery_str}")
                        try:
                            new_subquery_tokens = [
                                Token(Whitespace, ' '),
                                Token(Punctuation, '(')
                            ] + sqlparse.parse(filtered_subquery_str)[0].tokens + [Token(Punctuation, ')')]
                            new_where_tokens.extend(
                                [token] + new_subquery_tokens)
                            logger.debug(
                                f"Added filtered subquery to WHERE clause: {new_where_tokens[-len(new_subquery_tokens)-1:]}")
                        except Exception as e:
                            logger.error(
                                f"Error parsing filtered subquery: {e}")
                            # Fallback to original subquery with space
                            new_where_tokens.extend(
                                [token, Token(Whitespace, ' '), next_token[1]])
                        i += 2  # Skip the next token as we've handled it
                    else:
                        logger.debug(
                            "No parenthesis after 'IN', adding 'IN' token as is")
                        new_where_tokens.append(token)
                elif isinstance(token, Parenthesis):
                    logger.debug("Found parenthesis, checking for subquery")
                    subquery = token.tokens[1:-1]
                    subquery_str = ' '.join(str(t) for t in subquery)
                    if self._contains_subquery(sqlparse.parse(subquery_str)[0]):
                        logger.debug("Parenthesis contains subquery")
                        filtered_subquery = self._apply_filter_recursive(
                            sqlparse.parse(subquery_str)[0], client_id)
                        filtered_subquery_str = str(filtered_subquery)
                        logger.debug(
                            f"Filtered subquery: {filtered_subquery_str}")
                        try:
                            new_subquery_tokens = sqlparse.parse(
                                f"({filtered_subquery_str})")[0].tokens
                            new_where_tokens.extend(new_subquery_tokens)
                            logger.debug(
                                f"Added filtered subquery to WHERE clause: {new_where_tokens[-len(new_subquery_tokens):]}")
                        except Exception as e:
                            logger.error(
                                f"Error parsing filtered subquery: {e}")
                            # Fallback to original subquery
                            new_where_tokens.append(token)
                    else:
                        logger.debug(
                            "Parenthesis does not contain subquery, adding as is")
                        new_where_tokens.append(token)
                else:
                    new_where_tokens.append(token)
                i += 1

            # Add the client filter for the main table
            try:
                logger.debug(f"WHERE clause: {where_clause}")
                logger.debug(f"WHERE clause type: {type(where_clause)}")
                main_table = self._extract_main_table(where_clause)
                logger.debug(f"Main table extracted: {main_table}")
                if main_table:
                    main_table_filter = self._generate_client_filter(
                        main_table, client_id)
                    logger.debug(
                        f"Generated main table filter: {main_table_filter}")
                    if main_table_filter:
                        filter_tokens = [
                            Token(Whitespace, ' '),
                            Token(Keyword, 'AND'),
                            Token(Whitespace, ' ')
                        ] + sqlparse.parse(main_table_filter)[0].tokens
                        new_where_tokens.extend(filter_tokens)
                        logger.debug(
                            f"Added main table filter to WHERE clause: {filter_tokens}")
            except Exception as e:
                logger.error(f"Error adding main table filter: {e}")

            where_clause.tokens = new_where_tokens
            logger.debug(f"Final updated WHERE clause: {where_clause}")
            return where_clause

    def _generate_client_filter(self, table_name, client_id):
        matching_table = self._find_matching_table(table_name)
        if matching_table:
            client_id_column = self.client_tables[matching_table]
            return f'{self._quote_identifier(table_name)}.{self._quote_identifier(client_id_column)} = {client_id}'
        return None

    def _extract_main_query(self, parsed):
        logger.debug("Extracting main query")
        main_query_tokens = []
        main_query_started = False

        for token in parsed.tokens:
            if main_query_started:
                main_query_tokens.append(token)
            elif token.ttype is DML and token.value.upper() == 'SELECT':
                main_query_started = True
                main_query_tokens.append(token)

        return TokenList(main_query_tokens)

    def _extract_main_table(self, where_clause):
        logger.debug("Extracting main table")
        if where_clause.parent is None:
            logger.debug("WHERE clause parent is None")
            return None
        for token in where_clause.parent.tokens:
            logger.debug(f"Examining token: {token}, type: {type(token)}")
            if isinstance(token, Identifier):
                logger.debug(f"Found main table: {token.get_real_name()}")
                return token.get_real_name()
        logger.debug("No main table found")
        return None
