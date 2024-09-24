from sqlparse.tokens import DML, Keyword, Whitespace
from sqlparse.sql import TokenList, Parenthesis, Function
import re

def _contains_subquery(parsed):
    tokens = parsed.tokens if hasattr(parsed, 'tokens') else [parsed]
    
    set_operations = {'UNION', 'INTERSECT', 'EXCEPT'}
    joins = {'JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'INNER JOIN'}
    case_end_keywords = {'WHEN', 'THEN', 'ELSE'}
    where_keywords = {'IN', 'EXISTS', 'ANY', 'ALL', 'NOT IN'}
    end_keywords = {'GROUP BY', 'HAVING', 'ORDER BY'}

    def is_subquery(token):
        if isinstance(token, Parenthesis):
            inner_tokens = token.tokens
            for inner_token in inner_tokens:
                if inner_token.ttype is not Whitespace:
                    return inner_token.ttype is DML and inner_token.value.upper() == 'SELECT'
        return False

    i = 0
    while i < len(tokens):
        token = tokens[i]

        if token.ttype is DML and token.value.upper() == 'SELECT':
            k = i + 1    
            while k < len(tokens) and not (tokens[k].ttype == Keyword and tokens[k].value.upper() == 'FROM'):
                k += 1

            from_index = k
            where_index = None
            k = from_index + 1 

            while k < len(tokens):
                if 'WHERE' in str(tokens[k]):
                    where_index = k
                    break
                k += 1
            end_index = where_index if where_index else len(tokens)

            for j in range(i + 1, from_index): # Between SELECT and FROM block
                next_token = tokens[j]
                if is_subquery(next_token):
                    return True
                if isinstance(next_token, Function):
                    if re.search(r'\bCASE\b(\s+WHEN\b.?\bTHEN\b.?)+(\s+ELSE\b.*?)?(?=\s+END\b)', str(next_token), re.DOTALL):
                        # Check for subquery within CASE statement
                        if any(is_subquery(t) for t in next_token.tokens):
                            return True

            for j in range(from_index + 1, end_index): # FROM block checking for subqueries inside
                next_token = tokens[j]
                if is_subquery(next_token):
                    return True
                if isinstance(next_token, Function):
                    if any(op in next_token.value.upper() for op in set_operations):  # Set operations
                        return True
                if any(join in str(next_token).upper() for join in joins):  # Joins
                    if j+1 < len(tokens) and is_subquery(tokens[j+1]):
                        return True

            if where_index: # Proceed only if WHERE exists
                for j in range(where_index + 1, len(tokens)):
                    next_token = tokens[j]
                    if next_token.ttype == Keyword and next_token.value.upper() in where_keywords:
                        if j+1 < len(tokens) and is_subquery(tokens[j+1]):
                            return True
                    elif is_subquery(next_token):
                        return True

        i += 1
    return False