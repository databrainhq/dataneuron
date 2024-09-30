import sqlparse
from sqlparse.sql import Token
from sqlparse.tokens import Keyword, DML, Whitespace, Newline
import re
from query_cleanup import _cleanup_whitespace


class SubqueryHandler:
    def __init__(self, query_filter=None, setop_query_filter=None):
        self.SQLQueryFilter = query_filter
        self.SetOP_QueryFilter = setop_query_filter
        self._cleanup_whitespace = _cleanup_whitespace
        self.client_id = 1


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
        join_statement = []
        joins = {'JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'INNER JOIN'}

        subquery_dict = {
            "inline subquery": [],
            "join subquery": [],
            "set operations": [],
        }

        join_found = False
        for element in FROM_block: # Separate block to find at least one occurence of JOIN
            if isinstance(element, Token) and element.ttype == Keyword and element.value.upper() in joins:
                join_found = True
                break                

        for i, element in enumerate(FROM_block):
            if join_found:
                if i == 1 and isinstance(element, Token) and element.ttype == Keyword and element.value.upper() in joins:
                    join_statement.append(str(FROM_block[i - 1]))
                    join_statement.append(str(FROM_block[i + 1]))  
                elif i > 1 and isinstance(element, Token) and element.ttype == Keyword and element.value.upper() in joins:
                    join_statement.append(str(FROM_block[i + 1]))

            elif not join_found:
                if re.match(r'\(\s*([\s\S]*?)\s*\)', str(element), re.DOTALL):
                    if re.findall(r'(UNION\s+ALL|UNION|INTERSECT\s+ALL|INTERSECT|EXCEPT\s+ALL|EXCEPT)', str(element), re.IGNORECASE | re.DOTALL):
                        subquery_dict["set operations"].append(f"({str(element)})")
                    elif re.match(r'\(\s*SELECT.*\)\s+\w+', str(element), re.IGNORECASE | re.DOTALL):
                        subquery_dict['inline subquery'].append(str(element)) 

        for stmt in join_statement:
            join_statement_str = self._cleanup_whitespace(str(stmt))
            if re.findall(r'\(\s*([\s\S]*?)\s*\)', join_statement_str):
                if re.findall(r'(UNION\s+ALL|UNION|INTERSECT\s+ALL|INTERSECT|EXCEPT\s+ALL|EXCEPT)', join_statement_str, re.IGNORECASE | re.DOTALL):
                    subquery_dict["set operations"].append(f"({join_statement_str})")
                elif re.match(r'\(\s*SELECT.*\)\s+\w+', join_statement_str, re.IGNORECASE | re.DOTALL):
                    subquery_dict['join subquery'].append(join_statement_str)


        non_setop_filtered_dict = {
            'subquery_list': subquery_dict['inline subquery'] + subquery_dict['join subquery'],
            'filtered_subquery': [], 
            'placeholder_value': []
        }
        setop_filtered_dict = {
            'subquery_list': subquery_dict['set operations'],
            'filtered_subquery': [], 
            'placeholder_value': []
        }

        for nsod in range(len(non_setop_filtered_dict['subquery_list'])):
            placeholder = f"<FROM_PLACEHOLDER_{nsod}>"
            filtered_subquery = self.SQLQueryFilter( sqlparse.parse(re.search(r'\(((?:[^()]+|\([^()]*\))*)\)', non_setop_filtered_dict['subquery_list'][nsod]).group(1) )[0], self.client_id )  

            non_setop_filtered_dict['placeholder_value'].append(placeholder)
            non_setop_filtered_dict['filtered_subquery'].append(filtered_subquery)

        for sod in range(len(setop_filtered_dict["subquery_list"])):
            placeholder = f"<FROM_PLACEHOLDER_{len(setop_filtered_dict['subquery_list']) + sod}>"
            non_setop_filtered_dict['subquery_list'].append(subquery_dict['set operations'][sod])
            non_setop_filtered_dict['placeholder_value'].append(placeholder)

            filtered_subquery = self.SetOP_QueryFilter(sqlparse.parse(re.search(r'^\((.*)\)(\s+AS\s+\w+)?;?$', subquery_dict['set operations'][sod]).group(1))[0], self.client_id)
            non_setop_filtered_dict['filtered_subquery'].append(filtered_subquery)

        filtered_dict = {
            'subquery_list': non_setop_filtered_dict['subquery_list'] + setop_filtered_dict['subquery_list'],
            'filtered_subquery': non_setop_filtered_dict['filtered_subquery'] + setop_filtered_dict['filtered_subquery'], 
            'placeholder_value': non_setop_filtered_dict['placeholder_value'] + setop_filtered_dict['placeholder_value']
        }

        return filtered_dict


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
                k = from_index + 1 
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
        FROM_dict = self.FROM_subquery(from_block)
        subquery_dict = {
            "subqueries": SELECT_dict['subquery_list'] + FROM_dict['subquery_list'] + WHERE_dict['subquery_list'] + END_dict['subquery_list'],
            "filtered subqueries": SELECT_dict['filtered_subquery'] + FROM_dict['filtered_subquery'] + WHERE_dict['filtered_subquery'] + END_dict['filtered_subquery'],
            "placeholder names": SELECT_dict['placeholder_value'] + FROM_dict['placeholder_value'] + WHERE_dict['placeholder_value'] + END_dict['placeholder_value']
        }

        for i in range(len(subquery_dict['filtered subqueries'])):
            pattern = re.search(r'\(((?:[^()]+|\([^()]*\))*)\)\s*(?:AS\s+)?(\w+)?', subquery_dict['subqueries'][i], re.IGNORECASE)
            if pattern:
                subquery_with_alias = pattern.group(1)

                mainquery_str = str(parsed).replace(subquery_with_alias, subquery_dict["placeholder names"][i]) if i == 0 \
                                else mainquery_str.replace(subquery_with_alias, subquery_dict["placeholder names"][i])

            if len(subquery_dict['subqueries']) == 1:
                filtered_mainquery = self.SQLQueryFilter(sqlparse.parse(mainquery_str)[0], self.client_id)
            else:
                if i == 0:
                    filtered_mainquery = mainquery_str
                elif i == len(subquery_dict['subqueries']) - 1:
                    filtered_mainquery = self.SQLQueryFilter(sqlparse.parse(mainquery_str)[0], self.client_id)            

        for placeholder, filtered_subquery in zip(subquery_dict['placeholder names'], subquery_dict['filtered subqueries']):
                filtered_mainquery = filtered_mainquery.replace(placeholder, filtered_subquery)

        return filtered_mainquery