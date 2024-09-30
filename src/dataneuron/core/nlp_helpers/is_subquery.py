import re
from sqlparse.sql import Token
from sqlparse.tokens import DML, Keyword, Whitespace, Newline
from query_cleanup import _cleanup_whitespace

def _contains_subquery(parsed):
    tokens = parsed.tokens if hasattr(parsed, 'tokens') else [parsed]
    end_keywords = {'GROUP BY', 'HAVING', 'ORDER BY'}
    joins = {'JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'INNER JOIN'}
    where_keywords = {'IN', 'NOT IN', 'EXISTS', 'ALL', 'ANY'}
    where_keyword_pattern = '|'.join(where_keywords)

    select_index = None
    from_index = None
    where_index = None
    end_index = None

    select_block = []
    from_block = []
    where_block = []
    end_keywords_block = []
    results = []
    join_statement = []
    join_found = False

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

    for j in range(select_index + 1, from_index): # Between SELECT and FROM block
        select_block.append(_cleanup_whitespace(str(tokens[j])))

    select_elements = ' '.join(select_block).strip().split(',') # Split by commas to handle multiple elements in the SELECT block
    for element in select_elements:
        element = element.replace('\n', ' ').strip()  # Clean up any extra whitespace

        if re.search(r'\bCASE\b((\s+WHEN\b.*?\bTHEN\b.*?)+)(\s+ELSE\b.*)?(?=\s+END\b)', element, re.DOTALL):

            for match in re.findall(r'\bWHEN\b.*?\bTHEN\b.*?\bELSE\b.*?(?=\bWHEN\b|\bELSE\b|\bEND\b)', element, re.DOTALL): #Split them into WHEN, THEN and ELSE blocks: # Check for subquery inside WHEN THEN
                if re.search(r'\(.*?\bSELECT\b.*?\)', match, re.DOTALL):
                    results.append("Subquery exists inside CASE WHEN THEN ELSE block")

        elif '(' in element and ')' in element: # Find if any element has parenthesis
            if re.search(r'\(.*?\bSELECT\b.*?\)', element, re.DOTALL):
                results.append("Inline Subquery exists inside SELECT block")


    for j in range(from_index + 1, from_end):
        if isinstance(tokens[j], Token) and tokens[j].ttype not in [Whitespace, Newline]:
            from_block.append(tokens[j])

    for i, element in enumerate(from_block):
        if isinstance(element, Token) and element.ttype == Keyword and element.value.upper() in joins:
            join_found = True

            if i == 1:
                join_statement.append(str(from_block[i - 1]))
                join_statement.append(str(from_block[i + 1]))  
            elif i > 1:
                join_statement.append(str(from_block[i + 1]))

        elif not join_found and re.match(r'\(\s*([\s\S]*?)\s*\)', str(element), re.DOTALL):
            if re.findall(r'(UNION\s+ALL|UNION|INTERSECT\s+ALL|INTERSECT|EXCEPT\s+ALL|EXCEPT)', str(element), re.IGNORECASE | re.DOTALL):
                results.append("Contains set operation - Subquery found inside FROM block")
            elif re.match(r'\(\s*SELECT.*\)\s+\w+', str(element), re.IGNORECASE | re.DOTALL):
                results.append("Inline subquery inside FROM block") 

    if join_found:
        for stmt in join_statement:
            join_statement_str = _cleanup_whitespace(str(stmt))
            if re.findall(r'\(\s*([\s\S]*?)\s*\)', join_statement_str):
                if re.findall(r'(UNION\s+ALL|UNION|INTERSECT\s+ALL|INTERSECT|EXCEPT\s+ALL|EXCEPT)', join_statement_str, re.IGNORECASE | re.DOTALL):
                    results.append("Set operation - Subquery inside JOIN")
                elif re.match(r'\(\s*SELECT.*\)\s+\w+', join_statement_str, re.IGNORECASE | re.DOTALL):
                    results.append("Inline subquery inside JOIN")

    if where_index:
        for j in range(where_index, where_end): 
            where_block.append(_cleanup_whitespace(str(tokens[j]).strip('WHERE ')))

    for i in where_block:
        for clause in re.split(r'\bAND\b(?![^()]*\))', i):
            clause = clause.strip()

            if re.search(fr'\b({where_keyword_pattern})\b\s*\(.*?\bSELECT\b.*?\)', clause, re.DOTALL):
                found_keyword = re.search(fr'\b({where_keyword_pattern})\b', clause).group()
                results.append(f"Subquery with special keyword found in WHERE block: {found_keyword} \n")
            elif re.search(r'\(.*?\bSELECT\b.*?\)', clause, re.DOTALL):
                results.append("Inline subquery found in WHERE block \n")

    if end_index:
        for j in range(end_index, len(tokens)):
            if isinstance(tokens[j], Token) and tokens[j].ttype not in [Whitespace, Newline]:
                end_keywords_block.append(_cleanup_whitespace(str(tokens[j])))

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
                    if re.search(r'\(((?:[^()]+|\([^()]*\))*)\)\s*(?:AS\s+)?(\w+)?', str(endsubquery_block_str), re.IGNORECASE).group(1):
                        results.append("Subquery in END keywords")

    if len(results) >= 1:
        return True
    else:
        return False