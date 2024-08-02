from sqlparse.sql import Token, Where
from sqlparse.tokens import Keyword
from typing import Optional, List, Dict
from .sql_parser import WhereClauseModifier


class WhereClauseModifierImplementation(WhereClauseModifier):
    def modify_where_clause(self, where_clause: Optional[Token], new_condition: str) -> Token:
        if where_clause is None:
            return Token(Keyword, f"WHERE {new_condition}")

        if isinstance(where_clause, Where):
            existing_condition = str(where_clause).replace(
                "WHERE ", "", 1).strip()
            return Token(Keyword, f"WHERE {existing_condition} AND {new_condition}")

        # If it's just a Token, not a Where instance
        existing_condition = str(where_clause).replace("WHERE ", "", 1).strip()
        return Token(Keyword, f"WHERE {existing_condition} AND {new_condition}")
