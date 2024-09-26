import re
from sqlparse.sql import Token
from sqlparse.tokens import DML, Keyword, Whitespace, Newline
from query_cleanup import _cleanup_whitespace

def _contains_subquery(parsed):
    tokens = parsed.tokens if hasattr(parsed, 'tokens') else [parsed]

    set_operations = {'UNION', 'INTERSECT', 'EXCEPT'}
    joins = {'JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'INNER JOIN'}

    where_keywords = {'IN', 'NOT IN', 'EXISTS', 'ALL', 'ANY'}
    where_keyword_pattern = '|'.join(where_keywords)

    select_index = None
    from_index = None
    where_index = None
    end_index = None


    select_block = []
    from_block = []
    join_statement = []
    join_found = False
    where_block = []
    results = []

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

    select_elements = ' '.join(select_block).strip().split(',') # Split by commas to handle multiple elements in the SELECT block

    for element in select_elements:
        element = element.replace('\n', ' ').strip()  # Clean up any extra whitespace

        if re.search(r'\bCASE\b((\s+WHEN\b.*?\bTHEN\b.*?)+)(\s+ELSE\b.*)?(?=\s+END\b)', element, re.DOTALL):
            results.append("CASE Block exists in SELECT block - Checking if it has subquery in any of WHEN, THEN or ELSE blocks")

            for match in re.findall(r'\bWHEN\b.*?\bTHEN\b.*?\bELSE\b.*?(?=\bWHEN\b|\bELSE\b|\bEND\b)', element, re.DOTALL): #Split them into WHEN, THEN and ELSE blocks: # Check for subquery inside WHEN THEN
                if re.search(r'\(.*?\bSELECT\b.*?\)', match, re.DOTALL):
                    results.append("Subquery exists inside CASE WHEN THEN ELSE block")

        elif '(' in element and ')' in element: # Find if any element has parenthesis
            results.append("Inline element has parenthesis inside SELECT block - Checking if it has subquery")
            if re.search(r'\(.*?\bSELECT\b.*?\)', element, re.DOTALL):
                results.append("Inline Subquery exists inside SELECT block")

    for j in range(from_index + 1, from_end): # Between FROM and WHERE (or) end of query
        if isinstance(tokens[j], Token) and tokens[j].ttype not in [Whitespace, Newline]:
            from_block.append(tokens[j])

    for i, element in enumerate(from_block):
        if isinstance(element, Token) and element.ttype == Keyword and element.value.upper() in joins: # JOINs
            join_found = True
            if i == 1:
                join_statement.append(str(from_block[i - 1]))
                join_statement.append(str(from_block[i + 1]))  
            elif i > 1:
                join_statement.append(str(from_block[i + 1]))
        
        elif not join_found and re.match(r'\(\s*([\s\S]*?)\s*\)', str(element), re.DOTALL):
            results.append("Outer parentheses found inside FROM block - Checking if is an inline or contains set operation")

            if re.match(r'\(\s*SELECT.*UNION.*SELECT.*\)\s+\w+', str(element), re.IGNORECASE | re.DOTALL):
                results.append("(SELECT ... UNION .. SELECT...) - Contains set operation - Not a subquery inside FROM block")
            elif re.match(r'\(\s*\(\s*SELECT.*?FROM.*?\)\s*UNION\s*\(SELECT.*?FROM.*?(AS \w+)?\)\s*\)\s+AS\s+\w+', str(element), re.IGNORECASE | re.DOTALL):
                results.append("( (SELECT ...) UNION .. (SELECT...) ) - Contains set operation - Subquery found inside FROM block")
            elif re.match(r'\(\s*SELECT.*\)\s+\w+', str(element), re.IGNORECASE | re.DOTALL):
                results.append("Inline subquery inside FROM block")
        
    if join_found:
        results.append("JOIN operation found inside FROM - Checking if has subquery")
        for stmt in join_statement:
            join_statement_str = _cleanup_whitespace(str(stmt))
            if "(" in join_statement_str and ")" in join_statement_str:
                results.append("Parenthesis found inside JOIN - Checking if is a subquery")

                if re.match(r'\(\s*SELECT.*UNION.*SELECT.*\)\s+\w+', join_statement_str, re.IGNORECASE | re.DOTALL):
                    results.append("(SELECT ... UNION .. SELECT...) - Not a subquery inside JOIN")
                elif re.match(r'\(\s*\(\s*SELECT.*?FROM.*?\)\s*UNION\s*\(SELECT.*?FROM.*?(AS \w+)?\)\s*\)\s+AS\s+\w+', join_statement_str, re.IGNORECASE | re.DOTALL):
                    results.append("( (SELECT ...) UNION .. (SELECT...) ) - Subquery inside JOIN")
                elif re.match(r'\(\s*SELECT.*\)\s+\w+', join_statement_str, re.IGNORECASE | re.DOTALL):
                    results.append("Inline subquery inside JOIN")

    if where_index: # Between WHERE and end of query (or) End_keywords
        for j in range(where_index, len(tokens)): 
            where_block.append(_cleanup_whitespace(str(tokens[j]).strip('WHERE ')))

        for i in where_block:
            for clause in re.split(r'\bAND\b(?![^()]*\))', i):  # Splits into multiple statements if AND exists, else selects the single statement
                clause = clause.strip()

                # Check for the presence of any special keyword like IN, NOT IN, EXISTS, ALL, ANY
                if re.search(fr'\b({where_keyword_pattern})\b\s*\(.*?\bSELECT\b.*?\)', clause, re.DOTALL):
                    found_keyword = re.search(fr'\b({where_keyword_pattern})\b', clause).group()
                    results.append(f"Subquery with special keyword found in WHERE block: {found_keyword} \n")

                # Check for subquery using a SELECT statement in parentheses
                elif re.search(r'\(.*?\bSELECT\b.*?\)', clause, re.DOTALL):
                    results.append("Inline subquery found in WHERE block \n")

    if len(results) > 1:
        return True
    else:
        return False