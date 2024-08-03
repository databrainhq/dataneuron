from sqlparse.tokens import Keyword, CTE
from sqlparse.sql import IdentifierList, Identifier, Token, TokenList, Parenthesis, Where, Comparison
from sqlparse.tokens import Keyword, DML, Name, Whitespace, Punctuation
from typing import List, Dict, Optional

import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def is_cte_query(parsed):
    if parsed is None:
        logger.debug("Input is None")
        return False
    if not parsed.tokens:
        logger.debug("Empty query or no tokens")
        return False
    logger.debug(f"Checking if it is cte {parsed}")
    for token in parsed.tokens:
        if token.ttype in (Keyword, Keyword.CTE) and token.value.upper() == 'WITH':
            return True
        elif isinstance(token, TokenList):
            # Check if the first word in the token is exactly 'WITH'
            first_word = token.token_first(skip_cm=True)
            if first_word and first_word.value.upper() == 'WITH':
                logger.info(
                    f"CTE keyword 'WITH' found in TokenList: {first_word}")
                return True
    logger.info("No CTE keyword 'WITH' found")
    return False
