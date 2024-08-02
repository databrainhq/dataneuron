import sqlparse
from sqlparse.sql import TokenList
from sqlparse.tokens import Keyword, DML, Whitespace, Text
import logging

logger = logging.getLogger(__name__)


def extract_cte_definition(parsed):
    logger.debug("Extracting CTE definition")
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
                logger.debug(
                    f"No CTE found in the query. First non-whitespace token: {token.ttype} - {token.value}")
                return TokenList([])
        else:
            cte_tokens.append(token)
            if isinstance(token, sqlparse.sql.Parenthesis):
                parenthesis_count += token.value.count(
                    '(') - token.value.count(')')
            elif token.ttype is DML and token.value.upper() == 'SELECT' and parenthesis_count == 0:
                cte_tokens.pop()  # Remove the SELECT token from CTE
                break

    logger.debug(f"Extracted CTE tokens: {[str(t) for t in cte_tokens]}")
    return TokenList(cte_tokens)


def extract_main_query(parsed):
    logger.debug("Extracting main query")
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
    logger.debug(f"Filtering CTE part: {cte_part}")
    filtered_ctes = []

    for token in cte_part.tokens:
        if isinstance(token, sqlparse.sql.Identifier):
            cte_name = token.get_name()
            inner_query = token.tokens[-1]
            if isinstance(inner_query, sqlparse.sql.Parenthesis):
                # Remove the outer parentheses
                inner_query_str = str(inner_query)[1:-1]
                filtered_inner_query = filter_function(
                    sqlparse.parse(inner_query_str)[0], client_id)
                filtered_ctes.append(f"{cte_name} AS ({filtered_inner_query})")

    filtered_cte_str = "WITH " + ",\n".join(filtered_ctes)
    logger.debug(f"Filtered CTE: {filtered_cte_str}")
    return filtered_cte_str


def handle_cte_query(parsed, filter_function, client_id):
    logger.debug(f"Handling CTE query: {parsed}")
    cte_part = extract_cte_definition(parsed)
    main_query = extract_main_query(parsed)
    logger.debug(f"CTE part: {cte_part}")
    logger.debug(f"Main query part: {main_query}")

    if cte_part and main_query:
        try:
            filtered_cte = filter_cte(cte_part, filter_function, client_id)
            filtered_main = filter_function(main_query, client_id)
            logger.debug(f"Filtered CTE: {filtered_cte}")
            logger.debug(f"Filtered main query: {filtered_main}")
            final_result = f"{filtered_cte}\n{filtered_main}"
        except Exception as e:
            logger.error(f"Error during CTE filtering: {e}")
            final_result = str(parsed)
    else:
        logger.warning("Failed to separate CTE part and main query")
        final_result = str(parsed)

    logger.debug(f"Final result of CTE handling: {final_result}")
    return final_result
