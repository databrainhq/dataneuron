import re
import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Token, TokenList, Parenthesis, Where, Comparison
from sqlparse.tokens import Keyword, DML, Name, Whitespace, Punctuation
from typing import List, Dict, Optional
from .nlp_helpers.cte_handler import handle_cte_query
from .nlp_helpers.is_cte import is_cte_query


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

        is_cte = self._is_cte_query(parsed)

        if is_cte:
            return handle_cte_query(parsed, self._apply_filter_recursive, client_id)
        else:
            result = self._apply_filter_recursive(parsed, client_id)

        return self._cleanup_whitespace(str(result))

    def _apply_filter_recursive(self, parsed, client_id, cte_name: str = None):
        if self._is_cte_query(parsed):
            return handle_cte_query(parsed, self._apply_filter_recursive, client_id)

        for token in parsed.tokens:
            if isinstance(token, Token) and token.ttype is DML:
                if self._contains_set_operation(parsed):
                    return self._handle_set_operation(parsed, client_id, True, cte_name) if cte_name else self._handle_set_operation(parsed, client_id)
                elif self._contains_subquery(parsed):
                    return self._handle_subquery(parsed, client_id)
                else:
                    return self._apply_filter_to_single_query(str(parsed), client_id)            

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
        return False

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

    def _extract_where_clause_tables(self, parsed, tables_info):
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

    def _extract_cte_tables(self, parsed, tables_info):
        cte_start = next((i for i, token in enumerate(
            parsed.tokens) if token.ttype is Keyword and token.value.upper() == 'WITH'), None)
        if cte_start is not None:
            for token in parsed.tokens[cte_start:]:
                if isinstance(token, sqlparse.sql.Identifier) and token.has_alias():
                    cte_name = token.get_alias()
                    tables_info.append(
                        {'name': cte_name, 'schema': None, 'alias': None})
                    cte_query = token.tokens[-1]
                    if isinstance(cte_query, sqlparse.sql.Parenthesis):
                        # Remove outer parentheses and parse the CTE query
                        cte_parsed = sqlparse.parse(str(cte_query)[1:-1])[0]
                        # Recursively extract tables from the CTE query
                        self._extract_tables_info(cte_parsed, tables_info)
                elif token.ttype is DML and token.value.upper() == 'SELECT':
                    break

    def _extract_tables_info(self, parsed, tables_info=None):
        if tables_info is None:
            tables_info = []

        self._extract_from_clause_tables(parsed, tables_info)
        self._extract_where_clause_tables(parsed, tables_info)
        self._extract_cte_tables(parsed, tables_info)

        return tables_info

    def _extract_nested_subqueries(self, parsed, tables_info):
        for token in parsed.tokens:
            if isinstance(token, Identifier) and token.has_alias():
                if isinstance(token.tokens[0], Parenthesis):
                    subquery = token.tokens[0].tokens[1:-1]
                    subquery_str = ' '.join(str(t) for t in subquery)
                    subquery_parsed = sqlparse.parse(subquery_str)[0]
                    self._extract_from_clause_tables(
                        subquery_parsed, tables_info)
                    self._extract_where_clause_tables(
                        subquery_parsed, tables_info)
                    self._extract_nested_subqueries(
                        subquery_parsed, tables_info)

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

        where_index = next((i for i, token in enumerate(parsed.tokens)
                            if token.ttype is Keyword and token.value.upper() == 'WHERE'), None)

        if where_index is not None:
            # Find the end of the existing WHERE clause
            end_where_index = len(parsed.tokens) - 1
            for i in range(where_index + 1, len(parsed.tokens)):
                token = parsed.tokens[i]
                if token.ttype is Keyword and token.value.upper() in ('GROUP', 'ORDER', 'LIMIT'):
                    end_where_index = i - 1
                    break

            # Insert our condition at the end of the existing WHERE clause
            parsed.tokens.insert(end_where_index + 1, Token(Whitespace, ' '))
            parsed.tokens.insert(end_where_index + 2, Token(Keyword, 'AND'))
            parsed.tokens.insert(end_where_index + 3, Token(Whitespace, ' '))
            parsed.tokens.insert(end_where_index + 4,
                                 Token(Name, where_clause))
        else:
            # Find the position to insert the WHERE clause
            insert_position = len(parsed.tokens)
            for i, token in enumerate(parsed.tokens):
                if token.ttype is Keyword and token.value.upper() in ('GROUP', 'ORDER', 'LIMIT'):
                    insert_position = i
                    break

            # Insert the new WHERE clause
            parsed.tokens.insert(insert_position, Token(Whitespace, ' '))
            parsed.tokens.insert(insert_position + 1, Token(Keyword, 'WHERE'))
            parsed.tokens.insert(insert_position + 2, Token(Whitespace, ' '))
            parsed.tokens.insert(insert_position + 3,
                                 Token(Name, where_clause))

        return str(parsed)

    def _handle_set_operation(self, parsed, client_id, is_cte: bool = False, cte_name: str = None):
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
            if is_cte:
                filtered_stmt = self._apply_filter_to_single_CTE_query(stmt, client_id, cte_name)
                filtered_statements.append(filtered_stmt)
                print(f"Filtered statement: {filtered_stmt}")
            else:
                match = re.search(r'\(([^()]*)\)', stmt)
                if match:
                    extracted_part = match.group(1)
                    filtered_stmt = stmt.replace(extracted_part, self._apply_filter_to_single_query(extracted_part, client_id))
                    filtered_statements.append(filtered_stmt)
                    #print(f"Filtered statement: {filtered_stmt}")
                else:
                    filtered_stmt = self._apply_filter_to_single_query(stmt, client_id)
                    filtered_statements.append(filtered_stmt)
                    #print(f"Filtered statement: {filtered_stmt}")

        # Reconstruct the query
        result = f" {set_operation} ".join(filtered_statements)
        print(f"Final result: {result}")
        return result

    def _apply_filter_to_single_query(self, sql_query: str, client_id: int) -> str:

        parts = sql_query.split(' GROUP BY ')
        main_query = parts[0]
        group_by = f" GROUP BY {parts[1]}" if len(parts) > 1 else ""

        parsed = sqlparse.parse(main_query)[0]
        tables_info = self._extract_tables_info(parsed)

        filters = []
        for table_info in tables_info:
            table_name = table_info['name']
            table_alias = table_info['alias']
            schema = table_info['schema']

            matching_table = self._find_matching_table(table_name, schema)

            if matching_table and matching_table not in self.filtered_tables:
                client_id_column = self.client_tables[matching_table]
                table_reference = table_alias or table_name
                filters.append(
                    f'{self._quote_identifier(table_reference)}.{self._quote_identifier(client_id_column)} = {client_id}')
                self.filtered_tables.add(matching_table)

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
        tokens = parsed.tokens if hasattr(parsed, 'tokens') else [parsed] 
        
        set_operations = {'UNION', 'INTERSECT', 'EXCEPT'}
        joins = {'JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'INNER JOIN'}
        case_end_keywords = {'WHEN', 'THEN', 'ELSE'}
        where_keywords = {'IN', 'EXISTS', 'ANY', 'ALL', 'NOT IN'}
        end_keywords = {'GROUP BY', 'HAVING', 'ORDER BY'}

        i = 0
        while i < len(tokens):
            token = tokens[i]

            if token.ttype is DML and token.value.upper() == 'SELECT':
                k = i + 1    
                while k < len(tokens) and not (tokens[k].ttype == Keyword and tokens[k].value.upper() == 'FROM'):  # Find the index of the FROM clause
                    k += 1

                from_index = k
                where_index = None
                k = from_index + 1 

                while k < len(tokens):
                    if tokens[k].ttype == Keyword and tokens[k].value.upper() == 'WHERE': # Find the WHERE clause if any
                        where_index = k
                        break
                    k += 1
                end_index = where_index if where_index else len(tokens)

                for j in range(i + 1, k): # Between SELECT and FROM block
                    next_token = tokens[j]
                    if "(" in str(next_token) and ")" in str(next_token):
                        if re.search(r'\bCASE\b(\s+WHEN\b.*?\bTHEN\b.*?)+(\s+ELSE\b.*?)?(?=\s+END\b)', str(next_token), re.DOTALL):
                            return True
                        else:
                            return True

                for j in range(from_index + 1, end_index): # FROM block checking for subqueries inside
                    next_token = str(tokens[j]).upper()
                    if "(" in next_token and ")" in next_token:
                        if any(op in next_token for op in set_operations):  # Set operations
                            return True
                        elif any(join in next_token for join in joins):  # Joins
                            return True # This condition verifies that at least one statement in JOINs is a subquery
                        else:
                            return True  # Inline subquery

                if where_index: # Procced only if WHERE exists
                    for j in range(where_index + 1, len(tokens)):
                        next_token = tokens[j]
                        token_str = str(next_token).upper()
                        if "(" in token_str and ")" in token_str:  # Inline subquery
                            return True
                        if str(next_token).startswith("CASE"):  # CASE END block inside WHERE
                            case_token_list = [t for t in TokenList(next_token).flatten() if t.ttype == Keyword]
                            if any(k.value.upper() in case_end_keywords for k in case_token_list):
                                return True
                        if next_token.ttype == Keyword and next_token.value.upper() in where_keywords:  # WHERE keywords
                            return True

                for j in range(end_index, len(tokens)): # WHERE block checking for subqueries
                    next_token = tokens[j]
                    token_str = str(next_token).upper()
                    if next_token.ttype == Keyword and next_token.value.upper() in end_keywords:
                        for m in range(j + 1, len(tokens)):
                            after_end_keyword_token = tokens[m]
                            after_end_token_str = str(after_end_keyword_token).upper()
                            if "(" in after_end_token_str and ")" in after_end_token_str:  # Inline subquery
                                return True
                            if after_end_keyword_token.ttype == Keyword and after_end_keyword_token.value.upper() in where_keywords:  # Keywords like IN, EXISTS, etc.
                                return True
                            if str(after_end_keyword_token).startswith("CASE"):  # CASE END block after GROUP BY, etc.
                                case_token_list = [t for t in TokenList(after_end_keyword_token).flatten() if t.ttype == Keyword]
                                if any(k.value.upper() in case_end_keywords for k in case_token_list):
                                    return True
            i += 1
        return None

    def _cleanup_whitespace(self, query: str) -> str:
        # Split the query into lines
        lines = query.split('\n')
        cleaned_lines = []
        for line in lines:
            # Remove leading/trailing whitespace from each line
            line = line.strip()
            # Replace multiple spaces with a single space, but not in quoted strings
            line = re.sub(r'\s+(?=(?:[^\']*\'[^\']*\')*[^\']*$)', ' ', line)
            # Ensure single space after commas, but not in quoted strings
            line = re.sub(
                r'\s*,\s*(?=(?:[^\']*\'[^\']*\')*[^\']*$)', ', ', line)
            cleaned_lines.append(line)
        # Join the lines back together
        return '\n'.join(cleaned_lines)

    def _handle_subquery(self, parsed, client_id):
        result = []
        tokens = parsed.tokens if hasattr(parsed, 'tokens') else [parsed]
        mainquery = []

        for token in tokens:
            if isinstance(token, Identifier) and token.has_alias():
                if isinstance(token.tokens[0], Parenthesis):
                    mainquery.append(" PLACEHOLDER ")
                    subquery = token.tokens[0].tokens[1:-1]
                    subquery_str = ' '.join(str(t) for t in subquery)
                    filtered_subquery = self._apply_filter_recursive(
                        sqlparse.parse(subquery_str)[0], client_id)
                    alias = token.get_alias()
                    AS_keyword = next((t for t in token.tokens if t.ttype == sqlparse.tokens.Keyword and t.value.upper() == 'AS'), None) # Checks for existence of 'AS' keyword

                    if AS_keyword:
                        result.append(f"({filtered_subquery}) AS {alias}")
                    else:
                        result.append(f"({filtered_subquery}) {alias}")
                else:
                    mainquery.append(str(token))

            elif isinstance(token, Parenthesis):
                mainquery.append(" PLACEHOLDER ")
                subquery = token.tokens[1:-1]
                subquery_str = ' '.join(str(t) for t in subquery)
                filtered_subquery = self._apply_filter_recursive(
                    sqlparse.parse(subquery_str)[0], client_id)
                result.append(f"({filtered_subquery})")

            elif isinstance(token, Where) and 'IN' in str(parsed):
                try:
                    filtered_where = self._handle_where_subqueries(
                        token, client_id)
                    result.append(str(filtered_where))
                except Exception as e:
                    result.append(str(token))
            else:
                mainquery.append(str(token))

        mainquery = ''.join(mainquery).strip()
        if ' IN ' in str(parsed):
            return f"{mainquery} {result[0]}"
        else:
            filtered_mainquery = self._apply_filter_to_single_query(mainquery, client_id)
            query = filtered_mainquery.replace("PLACEHOLDER", result[0])
            return query 

    def _handle_where_subqueries(self, where_clause, client_id):
        if self._is_cte_query(where_clause):
            cte_part = self._extract_cte_definition(where_clause)
            main_query = self._extract_main_query(where_clause)

            filtered_cte = self._apply_filter_recursive(cte_part, client_id)

            if 'WHERE' not in str(main_query).upper():
                main_query = self._add_where_clause_to_main_query(
                    main_query, client_id)

            return f"{filtered_cte} {main_query}"
        else:
            new_where_tokens = []
            i = 0
            while i < len(where_clause.tokens):
                token = where_clause.tokens[i]
                if token.ttype is Keyword and token.value.upper() == 'IN':
                    next_token = where_clause.token_next(i)
                    if next_token and isinstance(next_token[1], Parenthesis):
                        subquery = next_token[1].tokens[1:-1]
                        subquery_str = ' '.join(str(t) for t in subquery)
                        filtered_subquery = self._apply_filter_recursive(
                            sqlparse.parse(subquery_str)[0], client_id)
                        filtered_subquery_str = str(filtered_subquery)
                        try:
                            new_subquery_tokens = [
                                Token(Whitespace, ' '),
                                Token(Punctuation, '(')
                            ] + sqlparse.parse(filtered_subquery_str)[0].tokens + [Token(Punctuation, ')')]
                            new_where_tokens.extend(
                                [token] + new_subquery_tokens)
                        except Exception as e:
                            # Fallback to original subquery with space
                            new_where_tokens.extend(
                                [token, Token(Whitespace, ' '), next_token[1]])
                        i += 2  # Skip the next token as we've handled it
                    else:
                        new_where_tokens.append(token)
                elif isinstance(token, Parenthesis):
                    subquery = token.tokens[1:-1]
                    subquery_str = ' '.join(str(t) for t in subquery)
                    if self._contains_subquery(sqlparse.parse(subquery_str)[0]):
                        filtered_subquery = self._apply_filter_recursive(
                            sqlparse.parse(subquery_str)[0], client_id)
                        filtered_subquery_str = str(filtered_subquery)
                        try:
                            new_subquery_tokens = sqlparse.parse(
                                f"({filtered_subquery_str})")[0].tokens
                            new_where_tokens.extend(new_subquery_tokens)
                        except Exception as e:
                            # Fallback to original subquery
                            new_where_tokens.append(token)
                    else:
                        new_where_tokens.append(token)
                else:
                    new_where_tokens.append(token)
                i += 1

            # Add the client filter for the main table
            try:
                main_table = self._extract_main_table(where_clause)
                if main_table:
                    main_table_filter = self._generate_client_filter(
                        main_table, client_id)
                    if main_table_filter:
                        filter_tokens = [
                            Token(Whitespace, ' '),
                            Token(Keyword, 'AND'),
                            Token(Whitespace, ' ')
                        ] + sqlparse.parse(main_table_filter)[0].tokens
                        new_where_tokens.extend(filter_tokens)
            except Exception as e:
                print(f"error: {e}")

            where_clause.tokens = new_where_tokens
            return where_clause

    def _generate_client_filter(self, table_name, client_id):
        matching_table = self._find_matching_table(table_name)
        if matching_table:
            client_id_column = self.client_tables[matching_table]
            return f'{self._quote_identifier(table_name)}.{self._quote_identifier(client_id_column)} = {client_id}'
        return None

    def _extract_main_query(self, parsed):
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
        if where_clause.parent is None:
            return None
        for token in where_clause.parent.tokens:
            if isinstance(token, Identifier):
                return token.get_real_name()
        return None

    def _apply_filter_to_single_CTE_query(self, sql_query: str, client_id: int, cte_name: str) -> str:
        parts = sql_query.split(' GROUP BY ')
        main_query = parts[0]
        
        group_by = f" GROUP BY {parts[1]}" if len(parts) > 1 else ""
        parsed = sqlparse.parse(main_query)[0]
        tables_info = self._extract_tables_info(parsed)

        filters = []
        _table_ = []

        for table_info in tables_info:
                if table_info['name'] != cte_name:
                    table_dict = {
                        "name": table_info['name'],
                        "alias": table_info['alias'],
                        "schema": table_info['schema']
                    }
                    _table_.append(table_dict)
                    
        matching_table = self._find_matching_table(_table_[0]['name'], _table_[0]['schema'])

        if matching_table:
            client_id_column = self.client_tables[matching_table]
            table_reference = _table_[0]['alias'] or _table_[0]['name']

            filters.append(f'{self._quote_identifier(table_reference)}.{self._quote_identifier(client_id_column)} = {client_id}')

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