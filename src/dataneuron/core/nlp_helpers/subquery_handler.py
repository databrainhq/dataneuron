import sqlparse
from sqlparse.sql import Token
from sqlparse.tokens import Keyword, DML, Whitespace, Newline
import re
from sql_query_filter import SQLQueryFilter
from query_cleanup import _cleanup_whitespace

def _handle_subquery(parsed, client_id):
    tokens = parsed.tokens if hasattr(parsed, 'tokens') else [parsed]

    select_index = None
    from_index = None
    where_index = None
    end_index = None

    select_block = []
    from_block = []
    where_block = []

    def keyword_index(tokens):
        nonlocal select_index, from_index, where_index
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
                    if isinstance(tokens[k], Token) and 'WHERE' in str(tokens[k]):
                        where_index = k
                        break
                    k += 1
                
            i += 1
    
    keyword_index(tokens)       
    from_end = where_index if where_index is not None else len(tokens)

    for j in range(select_index + 1, from_index): # Between SELECT and FROM block
        select_block.append(_cleanup_whitespace(str(tokens[j])))

    for j in range(from_index + 1, from_end):
        if isinstance(tokens[j], Token) and tokens[j].ttype not in [Whitespace, Newline]:
            from_block.append(tokens[j])

    if where_index:
        for j in range(where_index, len(tokens)): 
            where_block.append(_cleanup_whitespace(str(tokens[j]).strip('WHERE ')))
        WHERE_dict = WHERE_subquery(parsed, client_id)
    
    SELECT_dict = SELECT_subquery(select_block, client_id)
    FROM_dict = FROM_subquery(from_index, client_id)

    subquery_dict = {
        "subqueries": SELECT_dict['subquery_list'] + FROM_dict['subquery_list'] + WHERE_dict['subquery_list'],
        "filtered subqueries": SELECT_dict['filtered_subquery'] + FROM_dict['filtered_subquery'] + WHERE_dict['filtered_subquery'],
        "placeholder names": SELECT_dict['placeholder_value'] + FROM_dict['placeholder_value'] + WHERE_dict['placeholder_value']
    }

    for i in range(len(subquery_dict['filtered subqueries'])):
        mainquery_str = str(parsed).replace(f"({subquery_dict['subqueries'][i]})", subquery_dict['placeholder names'][i]) if i == 0 else mainquery_str.replace(f"({subquery_dict['subqueries'][i]})", subquery_dict['placeholder names'][i])
        if len(subquery_dict['subqueries']) == 1:
            filtered_mainquery = SQLQueryFilter._apply_filter_recursive(sqlparse.parse(mainquery_str)[0], client_id) # Handle the case where there is only one subquery

        elif i == len(subquery_dict['subqueries']) - 1:
            filtered_mainquery = SQLQueryFilter._apply_filter_recursive(sqlparse.parse(mainquery_str)[0], client_id) # Apply filtering to the main query for the last iteration in case of multiple subqueries

        elif i == 0:
            filtered_mainquery = mainquery_str # For the first iteration, just keep the mainquery_str as it is

    for placeholder, filtered_subquery in zip(subquery_dict['placeholder names'], subquery_dict['filtered subqueries']):
            filtered_mainquery = filtered_mainquery.replace(placeholder, f"({str(filtered_subquery)})")

    print(filtered_mainquery)


def SELECT_subquery(SELECT_block, client_id):

    select_elements = ' '.join(SELECT_block).strip().split(',') # Split by commas to handle multiple elements in the SELECT block
    filtered_dict = {
        'subquery_list': [],
        'filtered_subquery': [], 
        'placeholder_value': []
    }

    for i, element in enumerate(select_elements):
        element = element.replace('\n', ' ').strip()  # Clean up any extra whitespace

        if re.search(r'\bCASE\b((\s+WHEN\b.*?\bTHEN\b.*?)+)(\s+ELSE\b.*)?(?=\s+END\b)', element, re.DOTALL):
            for match in re.findall(r'\bWHEN\b.*?\bTHEN\b.*?\bELSE\b.*?(?=\bWHEN\b|\bELSE\b|\bEND\b)', element, re.DOTALL): #Split them into WHEN, THEN and ELSE blocks: # Check for subquery inside WHEN THEN
                if re.search(r'\(.*?\bSELECT\b.*?\)', match, re.DOTALL):
                    filtered_dict['subquery_list'].append(match)

        elif '(' in element and ')' in element: # Find if any element has parenthesis
            if re.search(r'\(.*?\bSELECT\b.*?\)', element, re.DOTALL):
                filtered_dict['subquery_list'].append(element)

    for i, subquery in enumerate(filtered_dict['subquery_list']): # Apply filters to extracted subqueries
        placeholder = f"<SELECT_PLACEHOLDER_{i}>"
        filtered_subquery = SQLQueryFilter._apply_filter_to_single_query(subquery, client_id)
        filtered_dict['placeholder_value'].append(placeholder)
        filtered_dict['filtered_subquery'].append(filtered_subquery)

    return filtered_dict

def FROM_subquery(FROM_block, client_id):
    join_found = False
    join_statement = []
    joins = {'JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'INNER JOIN'}
    set_operations = {'UNION', 'INTERSECT', 'EXCEPT'}
    subquery_dict = {
        "inline subquery": [],
        "join subquery": [],
        "set operations": [],
    }

    for i, element in enumerate(FROM_block):
            if isinstance(element, Token) and element.ttype == Keyword and element.value.upper() in joins: # JOINs
                join_found = True
                if i == 1:
                    join_statement.append(str(FROM_block[i - 1]))
                    join_statement.append(str(FROM_block[i + 1]))  
                elif i > 1:
                    join_statement.append(str(FROM_block[i + 1]))
            
            elif not join_found and re.match(r'\(\s*([\s\S]*?)\s*\)', str(element), re.DOTALL):
                if re.match(r'\(\s*\(\s*SELECT.*?FROM.*?\)\s*UNION\s*\(SELECT.*?FROM.*?(AS \w+)?\)\s*\)\s+AS\s+\w+', str(element), re.IGNORECASE | re.DOTALL):
                    subquery_dict['set operations'].append(str(element))
                elif re.match(r'\(\s*SELECT.*\)\s+\w+', str(element), re.IGNORECASE | re.DOTALL):
                    subquery_dict['inline subquery'].append(str(element)) 
    if join_found:
        for stmt in join_statement:
            join_statement_str = _cleanup_whitespace(str(stmt))
            if "(" in join_statement_str and ")" in join_statement_str:
                if re.match(r'\(\s*\(\s*SELECT.*?FROM.*?\)\s*UNION\s*\(SELECT.*?FROM.*?(AS \w+)?\)\s*\)\s+AS\s+\w+', join_statement_str, re.IGNORECASE | re.DOTALL):
                    subquery_dict['set operations'].append(join_statement_str)
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
        filtered_subquery = SQLQueryFilter._apply_filter_recursive(
            sqlparse.parse(non_setop_filtered_dict['subquery_list'][nsod])[0], client_id)
        non_setop_filtered_dict['placeholder_value'].append(placeholder)
        non_setop_filtered_dict['filtered_subquery'].append(filtered_subquery)

    for sod in range(len(setop_filtered_dict['set operations'])):
        placeholder = f"<FROM_PLACEHOLDER_{len(setop_filtered_dict['subquery_list']) + sod}>"
        non_setop_filtered_dict['subquery_list'].append(subquery_dict['set operations'][sod])
        non_setop_filtered_dict['placeholder_value'].append(placeholder)
        filtered_subquery = SQLQueryFilter._handle_set_operation(
            sqlparse.parse(subquery_dict['set operations'][sod])[0], client_id)
        non_setop_filtered_dict['filtered_subquery'].append(filtered_subquery)

    filtered_dict = {
        'subquery_list': non_setop_filtered_dict['subquery_list'] + setop_filtered_dict['subquery_list'],
        'filtered_subquery': non_setop_filtered_dict['filtered_subquery'] + setop_filtered_dict['filtered_subquery'], 
        'placeholder_value': non_setop_filtered_dict['filtered_subquery'] + setop_filtered_dict['filtered_subquery']
    }

    return filtered_dict


def WHERE_subquery(WHERE_block, client_id):

    where_keywords = {'IN', 'NOT IN', 'EXISTS', 'ALL', 'ANY'}
    where_keyword_pattern = '|'.join(where_keywords)
    filtered_dict = {
        'subquery_list': [],
        'filtered_subquery': [], 
        'placeholder_value': []
    }

    for i in WHERE_block:
        for clause in re.split(r'\bAND\b(?![^()]*\))', i):  # Splits into multiple statements if AND exists, else selects the single statement
            clause = clause.strip()

            # Check for the presence of any special keyword like IN, NOT IN, EXISTS, ALL, ANY
            if re.search(fr'\b({where_keyword_pattern})\b\s*\(.*?\bSELECT\b.*?\)', clause, re.DOTALL):
                filtered_dict['subquery_list'].append(clause)

            # Check for subquery using a SELECT statement in parentheses
            elif re.search(r'\(.*?\bSELECT\b.*?\)', clause, re.DOTALL):
                filtered_dict['subquery_list'].append(clause)

    for j in range(len(filtered_dict['subquery_list'])):
        placeholder = f"<WHERE_PLACEHOLDER_{j}>"
        filtered_subquery = SQLQueryFilter._apply_filter_recursive(sqlparse.parse(filtered_dict['subquery_list'][j])[0], client_id)
        filtered_dict['placeholder_value'].append(placeholder)
        filtered_dict['filtered_subquery'].append(filtered_subquery)

    return filtered_dict

    
    