import sqlparse
from sqlparse.sql import TokenList
from sqlparse.tokens import Keyword, DML, Whitespace, Text


def extract_cte_definition(parsed):
    cte_tokens = []
    cte_started = False
    parenthesis_count = 0

    for token in parsed.tokens:
        if token.is_whitespace or token.ttype in (Text, Text.Whitespace, Text.Whitespace.Newline):
            # Skip all types of whitespace
            continue

        if not cte_started:
            if (token.ttype is Keyword or token.ttype is Keyword.CTE) and token.value.upper() == 'WITH':
                cte_started = True
                cte_tokens.append(token)
            else:
                # If we encounter any non-whitespace token before 'WITH', there's no CTE
                return TokenList([])
        else:
            cte_tokens.append(token)
            if isinstance(token, sqlparse.sql.Parenthesis):
                parenthesis_count += token.value.count(
                    '(') - token.value.count(')')
            elif token.ttype is DML and token.value.upper() == 'SELECT' and parenthesis_count == 0:
                cte_tokens.pop()  # Remove the SELECT token from CTE
                break

    return TokenList(cte_tokens)


def extract_main_query(parsed):
    main_query_tokens = []
    main_query_started = False

    for token in parsed.tokens:
        if main_query_started:
            main_query_tokens.append(token)
        elif token.ttype is DML and token.value.upper() == 'SELECT':
            main_query_started = True
            main_query_tokens.append(token)

    return TokenList(main_query_tokens)


def filter_cte(cte_part, filter_function, client_id):
    filtered_ctes = []

    def process_cte(token):
        if isinstance(token, sqlparse.sql.Identifier):
            cte_name = token.get_name()
            inner_query = token.tokens[-1]
            if isinstance(inner_query, sqlparse.sql.Parenthesis):
                # Remove outer parentheses
                inner_query_str = str(inner_query)[1:-1]
                filtered_inner_query = filter_function(
                    sqlparse.parse(inner_query_str)[0], client_id)
                filtered_ctes.append(f"{cte_name} AS ({filtered_inner_query})")

    for token in cte_part.tokens:
        if isinstance(token, sqlparse.sql.IdentifierList):
            for subtoken in token.get_identifiers():
                process_cte(subtoken)
        else:
            process_cte(token)

    if filtered_ctes:
        filtered_cte_str = "WITH " + ",\n".join(filtered_ctes)
    else:
        filtered_cte_str = ""
    return filtered_cte_str


def handle_cte_query(parsed, filter_function, client_id):
    cte_part = extract_cte_definition(parsed)
    main_query = extract_main_query(parsed)

    if cte_part and main_query:
        try:
            filtered_cte = filter_cte(cte_part, filter_function, client_id)
            filtered_main = filter_function(main_query, client_id)

            if filtered_cte:
                final_result = f"{filtered_cte}\n{filtered_main}"
            else:
                final_result = filtered_main
        except Exception as e:
            final_result = str(parsed)
    else:
        final_result = str(parsed)

    return final_result
