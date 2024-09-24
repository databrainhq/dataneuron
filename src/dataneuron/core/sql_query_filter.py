import re
import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Token, TokenList, Parenthesis, Where, Comparison
from sqlparse.tokens import Keyword, DML, Name, Whitespace, Punctuation
from typing import List, Dict, Optional
from .nlp_helpers.cte_handler import handle_cte_query
from .nlp_helpers.is_cte import is_cte_query
from .nlp_helpers.is_subquery import _contains_subquery
from .nlp_helpers.subquery_handler import _handle_subquery


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
        else:
            for token in parsed.tokens:
                if isinstance(token, Token) and token.ttype is DML:
                    if self._contains_set_operation(parsed) and not _contains_subquery(parsed):
                        return self._handle_set_operation(parsed, client_id, True, cte_name) if cte_name else self._handle_set_operation(parsed, client_id)
                    elif _contains_subquery(parsed):
                        return _handle_subquery(parsed, client_id)
                    else:
                        return self._apply_filter_to_single_query(str(parsed), client_id)
                    
    def _contains_set_operation(self, parsed):
        set_operations = ('UNION', 'INTERSECT', 'EXCEPT')
        
        for token in parsed.tokens:
            if token.ttype is Keyword and (token.value.upper() in set_operations or token.value.upper() in {op + ' ALL' for op in set_operations}):
                return True
        return False
    
    def _handle_set_operation(self, parsed, client_id, is_cte: bool = False, cte_name: str = None):
        set_operations = {'UNION', 'INTERSECT', 'EXCEPT'}
        statements = []
        current_statement = []
        set_operation = None
        for token in parsed.tokens:
            if token.ttype is Keyword and (token.value.upper() in set_operations or token.value.upper() in {op + ' ALL' for op in set_operations}):
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
                else:
                    filtered_stmt = self._apply_filter_to_single_query(str(stmt), client_id)
                    filtered_statements.append(filtered_stmt)

        result = f" {set_operation} ".join(filtered_statements)
        return result
                
    def _apply_filter_to_single_query(self, sql_query: str, client_id: int) -> str:
        parts = sql_query.split(' GROUP BY ')
        main_query = parts[0]
        group_by = f" GROUP BY {parts[1]}" if len(parts) > 1 else ""

        tables_info = self._extract_tables_info(sqlparse.parse(main_query)[0])

        filters = []
        for table_info in tables_info:
            table_name = table_info['name']
            table_alias = table_info['alias']
            schema = table_info['schema']

            matching_table = self._find_matching_table(table_name, schema)

            if matching_table and matching_table not in self.filtered_tables:
                client_id_column = self.client_tables[matching_table]
                table_reference = table_alias or table_name
                filters.append(f'{self._quote_identifier(table_reference)}.{self._quote_identifier(client_id_column)} = {client_id}')
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
    
    def _find_matching_table(self, table_name: str, schema: Optional[str] = None) -> Optional[str]:
        possible_names = [
            f"{schema}.{table_name}" if schema else table_name,
            table_name,
        ] + [f"{s}.{table_name}" for s in self.schemas]

        for name in possible_names:
            if self._case_insensitive_get(self.client_tables, name) is not None:
                return name
        return None
    
    def _quote_identifier(self, identifier: str) -> str:
        return f'"{identifier}"'
    
    def _strip_quotes(self, identifier: str) -> str:
        return identifier.strip('"').strip("'").strip('`')
    
    def _case_insensitive_get(self, dict_obj: Dict[str, str], key: str) -> Optional[str]:
        if self.case_sensitive:
            return dict_obj.get(key)
        return next((v for k, v in dict_obj.items() if k.lower() == key.lower()), None)
    
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

    def _extract_tables_info(self, parsed, tables_info=None):
        if tables_info is None:
            tables_info = []

        self._extract_from_clause_tables(parsed, tables_info)
        self._extract_where_clause_tables(parsed, tables_info)
        self._extract_cte_tables(parsed, tables_info)

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