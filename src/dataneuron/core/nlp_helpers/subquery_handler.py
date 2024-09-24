import sqlparse
from sqlparse.sql import TokenList
from sqlparse.tokens import Keyword, DML
from sql_query_filter import SQLQueryFilter
import re

def _handle_subquery(parsed, client_id):
    tokens = parsed.tokens if hasattr(parsed, 'tokens') else [parsed]
    i = 0
    while i < len(tokens):
        token = tokens[i]
        
        if token.ttype is DML and token.value.upper() == 'SELECT':
            select_start = i + 1
            k = select_start
            
            while k < len(tokens) and not (tokens[k].ttype == Keyword and tokens[k].value.upper() == 'FROM'): # Find the index of the FROM clause
                k += 1
            
            from_index = k
            where_index = None
            k = from_index + 1
            
            while k < len(tokens): # Find the WHERE clause if any
                if tokens[k].ttype == Keyword and tokens[k].value.upper() == 'WHERE':
                    where_index = k
                    break
                k += 1
            
            end_index = len(tokens)
            
            SELECT_block = TokenList(tokens[select_start:from_index])
            FROM_block = TokenList(tokens[from_index + 1:where_index]) if where_index else TokenList(tokens[from_index + 1:end_index])
            WHERE_block = TokenList(tokens[where_index + 1:end_index]) if where_index else None
            
            i = end_index # Move the index to the end of the processed part
        else:
            i += 1
    
    SELECT_dict = SELECT_subquery(parsed, client_id)
    FROM_dict = FROM_subquery(parsed, client_id)
    WHERE_dict = WHERE_subquery(parsed, client_id)

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

    return filtered_mainquery


def SELECT_subquery(SELECT_block, client_id):
    subqueries = re.findall(r'\(([^()]+(?:\([^()]\))[^()]*)\)', str(SELECT_block))
    filtered_dict = {
        'subquery_list': subqueries,
        'filtered_subquery': [], 
        'placeholder_value': []
    }

    for i, subquery in enumerate(filtered_dict['subquery_list']): # Apply filters to extracted subqueries
        placeholder = f"<SELECT_PLACEHOLDER_{i}>"
        filtered_subquery = SQLQueryFilter._apply_filter_to_single_query(subquery, client_id)
        filtered_dict['placeholder_value'].append(placeholder)
        filtered_dict['filtered_subquery'].append(filtered_subquery)

    return filtered_dict


def FROM_subquery(FROM_block, client_id):
    joins = {'JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'INNER JOIN'}
    set_operations = {'UNION', 'INTERSECT', 'EXCEPT'}
    subquery_dict = {
        "inline subquery": [],
        "join subquery": [],
        "set operations": [],
    }

    FROM_block_tokens = [token for token in FROM_block.tokens if not token.is_whitespace]

    for i, token in enumerate(FROM_block_tokens):
        if token.ttype is Keyword and token.value.upper() in joins: # JOINs operations
            if i > 0 and "(" in str(FROM_block_tokens[i-1]) and ")" in str(FROM_block_tokens[i-1]): # Select only subqueries 
                subquery = FROM_block_tokens[i-1].value
                if subquery not in subquery_dict['join subquery']:
                    subquery_dict['join subquery'].append(subquery)

            if i < len(FROM_block_tokens) - 1 and "(" in str(FROM_block_tokens[i+1]) and ")" in str(FROM_block_tokens[i+1]):
                subquery = FROM_block_tokens[i+1].value
                if subquery not in subquery_dict['join subquery']:
                    subquery_dict['join subquery'].append(subquery)
                i += 1

    i = 0
    while i < len(FROM_block.tokens): # SET operation
        if FROM_block.tokens[i].ttype is Keyword and FROM_block.tokens[i].value.upper() in set_operations:
            subquery_dict['set operations'].append(str(FROM_block))
            break
        i += 1

    filtered_dict = {
        'subquery_list': subquery_dict['inline subquery'] + subquery_dict['join subquery'],
        'filtered_subquery': [], 
        'placeholder_value': []
    }

    for i in range(len(filtered_dict['subquery_list'])):
        placeholder = f"<FROM_PLACEHOLDER_{i}>"
        filtered_subquery = SQLQueryFilter._apply_filter_recursive(
            sqlparse.parse(filtered_dict['subquery_list'][i])[0], client_id
        )
        filtered_dict['placeholder_value'].append(placeholder)
        filtered_dict['filtered_subquery'].append(filtered_subquery)

    for j in range(len(subquery_dict['set operations'])):
        placeholder = f"<FROM_PLACEHOLDER_{len(filtered_dict['subquery_list']) + j}>"
        filtered_dict['subquery_list'].append(subquery_dict['set operations'][j])
        filtered_dict['placeholder_value'].append(placeholder)

        filtered_subquery = SQLQueryFilter._handle_set_operation(
            sqlparse.parse(subquery_dict['set operations'][j])[0], client_id
        )
        filtered_dict['filtered_subquery'].append(filtered_subquery)
    return filtered_dict


def WHERE_subquery(parsed, client_id):
    tokens = parsed.tokens if hasattr(parsed, 'tokens') else [parsed]
    i = 0

    subquery_dict = {
        "in_subquery": [],
        "not_in_subquery": [],
        "exists_subquery": [],
        "not_exists_subquery": [],
        "any_subquery": [],
        "all_subquery": [],
        "inline subquery": [],
    }

    def subquery_extractor(next_token):
        for t in next_token[1]:
            if t.ttype == sqlparse.tokens.Keyword and t.value.upper() == 'IN':
                next_token_in = next_token[1].token_next(next_token[1].token_index(t))
                if "(" in str(next_token_in) and ")" in str(next_token_in):
                    subquery_dict['in_subquery'].append(str(TokenList(next_token_in[0][1:-1])))

            elif t.ttype == sqlparse.tokens.Keyword and t.value.upper() == 'NOT' and next_token[1].token_next(next_token[1].token_index(t))[1].value.upper() == 'IN':
                next_token_not_in = next_token[1].token_next(next_token[1].token_index(t) + 1)
                if "(" in str(next_token_not_in) and ")" in str(next_token_not_in):
                    subquery_dict['not_in_subquery'].append(str(TokenList(next_token_not_in[0][1:-1])))

            elif t.ttype == sqlparse.tokens.Keyword and t.value.upper() == 'EXISTS':
                next_token_exists = next_token[1].token_next(next_token[1].token_index(t))
                if "(" in str(next_token_exists) and ")" in str(next_token_exists):
                    subquery_dict['exists_subquery'].append(str(TokenList(next_token_exists[0][1:-1])))

            elif t.ttype == sqlparse.tokens.Keyword and t.value.upper() == 'NOT' and next_token[1].token_next(next_token[1].token_index(t))[1].value.upper() == 'EXISTS':
                next_token_not_exists = next_token[1].token_next(next_token[1].token_index(t) + 1)
                if "(" in str(next_token_not_exists) and ")" in str(next_token_not_exists):
                    subquery_dict['not_exists_subquery'].append(str(TokenList(next_token_not_exists[0][1:-1])))

            elif t.ttype == sqlparse.tokens.Keyword and t.value.upper() == 'ANY':
                next_token_any = next_token[1].token_next(next_token[1].token_index(t))
                if "(" in str(next_token_any) and ")" in str(next_token_any):
                    subquery_dict['any_subquery'].append(str(TokenList(next_token_any[0][1:-1])))

            elif t.ttype == sqlparse.tokens.Keyword and t.value.upper() == 'ALL':
                next_token_all = next_token[1].token_next(next_token[1].token_index(t))
                if "(" in str(next_token_all) and ")" in str(next_token_all):
                    subquery_dict['all_subquery'].append(str(TokenList(next_token_all[0][1:-1])))

            elif "(" in str(t) and ")" in str(t):
                subquery_dict['inline subquery'].append(str(TokenList(t[0][1:-1])))

            else:
                SQLQueryFilter._apply_filter_to_single_query(str(parsed), client_id)

    while i < len(tokens):
        token = parsed.tokens[i]
        if token.ttype == sqlparse.tokens.Keyword and token.value.upper() == 'WHERE':
            next_token = parsed.token_next(i)
            subquery_extractor(next_token)
        i += 1
        
    filtered_dict = {
        'subquery_list':  subquery_dict['in_subquery'] + subquery_dict['not_in_subquery'] + 
                        subquery_dict['exists_subquery'] + subquery_dict['not_exists_subquery'] + 
                        subquery_dict['any_subquery'] + subquery_dict['all_subquery'] + 
                        subquery_dict['inline subquery'],
        'filtered_subquery': [],
        'placeholder_value': []
    }  

    for i in range(len(filtered_dict['subquery_list'])):
        placeholder = f"<WHERE_PLACEHOLDER_{i}>"
        filtered_subquery = SQLQueryFilter._apply_filter_recursive(sqlparse.parse(filtered_dict['subquery'][i])[0], client_id)
        filtered_dict['placeholder_value'].append(placeholder)
        filtered_dict['filtered_subquery'].append(filtered_subquery)

    return filtered_dict

    
    