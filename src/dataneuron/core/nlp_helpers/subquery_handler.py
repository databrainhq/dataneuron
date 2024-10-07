import sqlparse
from sqlparse.sql import Token
from sqlparse.tokens import Keyword, DML, Whitespace, Newline
import re
from query_cleanup import _cleanup_whitespace

class SubqueryHandler:
    def __init__(self, query_filter=None, setop_query_filter=None, matching_table_finder=None):
        self.SQLQueryFilter = query_filter
        self.SetOP_QueryFilter = setop_query_filter
        self._find_matching_table = matching_table_finder
        self._cleanup_whitespace = _cleanup_whitespace
        self.client_id = 1
        self.schemas=['main', 'inventory']

    def SELECT_subquery(self, SELECT_block):
        select_elements = ' '.join(SELECT_block).strip().split(',')
        filtered_dict = {
            'subquery_list': [],
            'filtered_subquery': [], 
            'placeholder_value': []
        }

        for element in select_elements:
            element = element.replace('\n', ' ').strip()

            # Detect CASE WHEN THEN ELSE
            case_match = re.search(r'\bCASE\b(.*?\bEND\b)', element, re.DOTALL)
            if case_match:
                case_block = case_match.group(1)
                when_then_else_blocks = re.findall(r'\bWHEN\b(.*?)\bTHEN\b(.*?)(?=\bWHEN\b|\bELSE\b|\bEND\b)', case_block, re.DOTALL)
                else_clause = re.search(r'\bELSE\b(.*?)(?=\bEND\b)', case_block, re.DOTALL)

                # Process WHEN-THEN pairs
                for when, then in when_then_else_blocks:
                    if re.search(r'\(.*?\bSELECT\b.*?\)', when, re.DOTALL):  # Check if WHEN has a subquery
                        filtered_dict['subquery_list'].append(when)
                    if re.search(r'\(.*?\bSELECT\b.*?\)', then, re.DOTALL):  # Check if THEN has a subquery
                        filtered_dict['subquery_list'].append(then)

                # Process ELSE clause if exists
                if else_clause and re.search(r'\(.*?\bSELECT\b.*?\)', else_clause.group(1), re.DOTALL):
                    filtered_dict['subquery_list'].append(else_clause.group(1))         

            # Handle simple subqueries outside CASE block
            elif '(' in element and ')' in element:
                if re.search(r'\(.*?\bSELECT\b.*?\)', element, re.DOTALL):
                    filtered_dict['subquery_list'].append(element)

        # Create placeholders and filter subqueries
        for i, subquery in enumerate(filtered_dict['subquery_list']):
            placeholder = f"<SELECT_PLACEHOLDER_{i}>"
            filtered_subquery = self.SQLQueryFilter(
                sqlparse.parse(
                    re.search(r'\(((?:[^()]+|\([^()]*\))*)\)', subquery).group(1)
                )[0], 
                self.client_id
            )
            filtered_dict['placeholder_value'].append(placeholder)
            filtered_dict['filtered_subquery'].append(filtered_subquery)

        return filtered_dict


    def FROM_subquery(self, FROM_block):
        joins = {'JOIN', 'LEFT JOIN', 'RIGHT JOIN'}
        join_found = False
        join_statements = []
        exit_early = False

        join_dict = {
                "matching_table": [],
                "filtered_matching_table": [],
                "alias": []
            }
        
        def _handle_joins():
            for i, token in enumerate(FROM_block):
                if join_found and isinstance(token, Token) and token.ttype == Keyword and token.value.upper() in joins:
                    previous_token = FROM_block[i - 1] if i > 0 else None
                    next_token = FROM_block[i + 1] if i + 1 < len(FROM_block) else None
                    if previous_token:
                        join_statements.append(previous_token.value.strip())
                    if next_token:
                        join_statements.append(next_token.value.strip())

            for statement in join_statements:
                join_statement_str = _cleanup_whitespace(statement)
                if self._find_matching_table(join_statement_str, self.schemas):

                    filtered_table = self.SQLQueryFilter(
                        sqlparse.parse(f'SELECT * FROM {join_statement_str}')[0], self.client_id)
                    
                    join_dict['filtered_matching_table'].append(f'({filtered_table})')
                    join_dict['alias'].append(f"AS {join_statement_str}")
                    join_dict['matching_table'].append(join_statement_str)

                else:
                    if re.match(r'\(\s*([\s\S]*?)\s*\)', join_statement_str):
                        if re.findall(r'(UNION\s+ALL|UNION|INTERSECT\s+ALL|INTERSECT|EXCEPT\s+ALL|EXCEPT)', join_statement_str, re.IGNORECASE | re.DOTALL):
                            match = re.search(r'\(((?:[^()]+|\([^()]*\))*)\)', join_statement_str)
                            inner_parentheses = match.group(1)
                            start, end = match.span()
                            alias = join_statement_str[end + 1:]  # +1 for WHITESPACEEEE

                            filtered_subquery = self.SetOP_QueryFilter(sqlparse.parse(inner_parentheses)[0], self.client_id)
                            join_dict['filtered_matching_table'].append(f'({filtered_subquery})')
                            join_dict['matching_table'].append(join_statement_str)
                            join_dict['alias'].append(alias)
                        
                        elif re.match(r'\(\s*SELECT.*?\)\s*(?:AS\s+)?(\w+)?', join_statement_str, re.IGNORECASE | re.DOTALL):
                            subquery_match = re.match(r'\(\s*SELECT.*?\)\s*(?:AS\s+)?(\w+)?', join_statement_str, re.IGNORECASE | re.DOTALL)
                            inner_parentheses = re.search(r'\(((?:[^()]+|\([^()]*\))*)\)', join_statement_str).group(1)
                            alias = subquery_match.group(1)

                            filtered_subquery = self.SetOP_QueryFilter(sqlparse.parse(inner_parentheses)[0], self.client_id)
                            join_dict['matching_table'].append(join_statement_str)
                            join_dict['filtered_matching_table'].append(f'({filtered_subquery})')
                            join_dict['alias'].append(f"AS {alias}")
        
        def _not_handle_joins():
            nonlocal exit_early
            for token in FROM_block:
                FROM_block_str = _cleanup_whitespace(str(token))
                if re.match(r'\(\s*([\s\S]*?)\s*\)', FROM_block_str) and re.findall(r'(UNION\s+ALL|UNION|INTERSECT\s+ALL|INTERSECT|EXCEPT\s+ALL|EXCEPT)', FROM_block_str, re.IGNORECASE | re.DOTALL):
                    match = re.search(r'\(((?:[^()]+|\([^()]*\))*)\)', FROM_block_str)
                    inner_parentheses = match.group(1)
                    start, end = match.span()
                    alias = FROM_block_str[end + 1:]  # +1 for WHITESPACEEEE

                    filtered_subquery = self.SetOP_QueryFilter(sqlparse.parse(inner_parentheses)[0], self.client_id)
                    join_dict['filtered_matching_table'].append(f'({filtered_subquery})')
                    join_dict['matching_table'].append(FROM_block_str)
                    join_dict['alias'].append(alias)

                elif re.match(r'\(\s*SELECT.*?\)\s*(?:AS\s+)?(\w+)?', FROM_block_str, re.IGNORECASE | re.DOTALL):
                    subquery_match = re.match(r'\(\s*SELECT.*?\)\s*(?:AS\s+)?(\w+)?', FROM_block_str, re.IGNORECASE | re.DOTALL)
                    inner_parentheses = re.search(r'\(((?:[^()]+|\([^()]*\))*)\)', FROM_block_str).group(1)
                    alias = subquery_match.group(1)

                    filtered_subquery = self.SQLQueryFilter(sqlparse.parse(inner_parentheses)[0], self.client_id)
                    join_dict['matching_table'].append(FROM_block_str)
                    join_dict['filtered_matching_table'].append(f'({filtered_subquery})')
                    join_dict['alias'].append(f"AS {alias}")
                
                elif self._find_matching_table(str(token), self.schemas):
                    exit_early = True           

        for token in FROM_block:
            if isinstance(token, Token) and token.ttype == Keyword and token.value.upper() in joins:
                join_found = True
                break
        if join_found:
            _handle_joins()
        else:
            _not_handle_joins()

        if exit_early:
            return 0
        else:
            reconstructed_from_clause = []
            for token in FROM_block:
                if isinstance(token, Token) and token.value.strip() in join_dict["matching_table"]:
                    table_index = join_dict["matching_table"].index(token.value.strip())
                    filtered_table = join_dict["filtered_matching_table"][table_index]
                    added_alias = join_dict["alias"][table_index]
                    reconstructed_from_clause.append(f"{filtered_table} {added_alias}")
                else:
                    reconstructed_from_clause.append(token.value.strip())
                    
            reconstructed_query = " ".join(reconstructed_from_clause)
            return reconstructed_query


    def WHERE_subquery(self, WHERE_block):
        where_keywords = {'IN', 'NOT IN', 'EXISTS', 'ALL', 'ANY'}
        where_keyword_pattern = '|'.join(where_keywords)
        filtered_dict = {
            'subquery_list': [],
            'filtered_subquery': [], 
            'placeholder_value': []
        }

        for i in WHERE_block:
            for clause in re.split(r'\bAND\b(?![^()]*\))', i):
                clause = clause.strip()

                if re.search(fr'\b({where_keyword_pattern})\b\s*\(.*?\bSELECT\b.*?\)', clause, re.DOTALL):
                    filtered_dict['subquery_list'].append(clause)
                elif re.search(r'\(.*?\bSELECT\b.*?\)', clause, re.DOTALL):
                    filtered_dict['subquery_list'].append(clause)

        for j in range(len(filtered_dict['subquery_list'])):
            placeholder = f"<WHERE_PLACEHOLDER_{j}>"
            filtered_subquery = self.SQLQueryFilter( sqlparse.parse( re.search(r'\(((?:[^()]+|\([^()]*\))*)\)', (filtered_dict['subquery_list'][j])).group(1) )[0], self.client_id )
            filtered_dict['placeholder_value'].append(placeholder)
            filtered_dict['filtered_subquery'].append(filtered_subquery)

        return filtered_dict
    
    def END_subqueries(self, end_keywords_block):
        end_keywords = {'GROUP BY', 'HAVING', 'ORDER BY'}
        
        # Dictionary to hold the result
        filtered_dict = {
            'subquery_list': [],
            'filtered_subquery': [],
            'placeholder_value': []
        }
        
        endsubquery_block = []
        count = 0
        indices = []
        
        for index, token in enumerate(end_keywords_block):
            if str(token).upper() in end_keywords:
                count += 1
                indices.append(index)
        
        if count >= 1: # If there is at least one end keyword
            for i in range(len(indices)):
                start_idx = indices[i] # Start and end indices of each block
                if i < len(indices) - 1:
                    end_idx = indices[i + 1]  # Until the next keyword
                else:
                    end_idx = len(end_keywords_block)  # Until the end of the block

                # Extract the block between start_idx and end_idx
                endsubquery_block = end_keywords_block[start_idx:end_idx]
                endsubquery_block_str = ' '.join(endsubquery_block)

                if re.search(r'\((SELECT [\s\S]*?)\)', str(endsubquery_block_str), re.IGNORECASE):
                    subquery_match = re.search(r'\(((?:[^()]+|\([^()]*\))*)\)\s*(?:AS\s+)?(\w+)?', str(endsubquery_block_str), re.IGNORECASE).group(1)
                    print(subquery_match)
                    filtered_dict['subquery_list'].append(subquery_match)
                    placeholder = f"<END_PLACEHOLDER_{len(filtered_dict['placeholder_value'])}>"
                    filtered_dict['filtered_subquery'].append(self.SQLQueryFilter(sqlparse.parse(subquery_match)[0], self.client_id))
                    filtered_dict['placeholder_value'].append(placeholder)      
                        
        return filtered_dict
    

    def handle_subquery(self, parsed):
        tokens = parsed.tokens if hasattr(parsed, 'tokens') else [parsed]
        end_keywords = {'GROUP BY', 'HAVING', 'ORDER BY'}

        select_index = None
        from_index = None
        where_index = None
        end_index = None

        select_block = []
        from_block = []
        where_block = []
        end_keywords_block = []

        i = 0
        while i < len(tokens):
            token = tokens[i]

            if isinstance(token, Token) and token.ttype is DML and token.value.upper() == 'SELECT':
                select_index = i
                k = i + 1
                while k < len(tokens) and not (isinstance(tokens[k], Token) and tokens[k].ttype == Keyword and tokens[k].value.upper() == 'FROM'):
                    k += 1

                from_index = k
                
                k += 1
                while k < len(tokens):
                    if isinstance(tokens[k], Token) and 'WHERE' in str(tokens[k]) and not \
                                        re.match(r'\(\s*SELECT.*?\bWHERE\b.*?\)', str(tokens[k])):
                        where_index = k
                    elif isinstance(tokens[k], Token) and str(tokens[k]) in end_keywords:
                        end_index = k
                        break

                    k += 1 
            i += 1
            
        where_end = end_index if end_index else len(tokens)
        from_end = min(
            index for index in [where_index, end_index] if index is not None) if any([where_index, end_index]) \
                else len(tokens)

        for j in range(select_index + 1, from_index):
            select_block.append(self._cleanup_whitespace(str(tokens[j])))

        for j in range(from_index + 1, from_end):
            if isinstance(tokens[j], Token) and tokens[j].ttype not in [Whitespace, Newline]:
                from_block.append(tokens[j])

        WHERE_dict = {'subquery_list': [], 'filtered_subquery': [], 'placeholder_value': []}  # For cases where WHERE_dict is empty and leads to [UnboundLocalError: cannot access local variable 'WHERE_dict' where it is not associated with a value]
        if where_index:
            for j in range(where_index, where_end): 
                where_block.append(self._cleanup_whitespace(str(tokens[j]).strip('WHERE ')))
            WHERE_dict = self.WHERE_subquery(where_block)

        END_dict = {'subquery_list': [], 'filtered_subquery': [], 'placeholder_value': []}
        if end_index:
            for j in range(end_index, len(tokens)):
                if isinstance(tokens[j], Token) and tokens[j].ttype not in [Whitespace, Newline]:
                    end_keywords_block.append(self._cleanup_whitespace(str(tokens[j])))
            END_dict = self.END_subqueries(end_keywords_block)
            
        SELECT_dict = self.SELECT_subquery(select_block)
        subquery_dict = {
            "subqueries": SELECT_dict['subquery_list'] + WHERE_dict['subquery_list'] + END_dict['subquery_list'],
            "filtered subqueries": SELECT_dict['filtered_subquery'] + WHERE_dict['filtered_subquery'] + END_dict['filtered_subquery'],
            "placeholder names": SELECT_dict['placeholder_value'] + WHERE_dict['placeholder_value'] + END_dict['placeholder_value']
        }
        FROM_filtering = self.FROM_subquery(from_block)


        for i in range(len(subquery_dict['filtered subqueries'])):
            pattern = re.search(r'\(((?:[^()]+|\([^()]*\))*)\)\s*(?:AS\s+)?(\w+)?', subquery_dict['subqueries'][i], re.IGNORECASE)
            if pattern:
                subquery_with_alias = pattern.group(1)
                mainquery_str = str(parsed).replace(subquery_with_alias, subquery_dict["placeholder names"][i]) if i == 0 \
                                else mainquery_str.replace(subquery_with_alias, subquery_dict["placeholder names"][i])
                
            
            if FROM_filtering == 0:
                if len(subquery_dict['subqueries']) == 1:
                    filtered_mainquery = self.SQLQueryFilter(sqlparse.parse(mainquery_str)[0], self.client_id)
                else:
                    if i == 0:
                        filtered_mainquery = mainquery_str
                    elif i == len(subquery_dict['subqueries']) - 1:
                        filtered_mainquery = self.SQLQueryFilter(sqlparse.parse(mainquery_str)[0], self.client_id)
            else:
                from_start = mainquery_str.upper().find('FROM')
                where_start = mainquery_str.upper().find('WHERE')

                if where_start == -1:  # If there's no WHERE clause
                    next_clause_starts = [mainquery_str.upper().find(clause) for clause in ['GROUP BY', 'HAVING', 'ORDER BY', 'LIMIT'] if mainquery_str.upper().find(clause) != -1]
                    where_start = min(next_clause_starts) if next_clause_starts else len(mainquery_str)
                    
                part_to_replace = mainquery_str[from_start:where_start].strip()
                filtered_mainquery = mainquery_str.replace(part_to_replace, f"FROM {FROM_filtering}")

        for placeholder, filtered_subquery in zip(subquery_dict['placeholder names'], subquery_dict['filtered subqueries']):
                filtered_mainquery = filtered_mainquery.replace(placeholder, filtered_subquery)
        
        return filtered_mainquery