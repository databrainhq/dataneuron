from .db_operations.factory import DatabaseFactory


def execute_query(query: str) -> str:
    db = DatabaseFactory.get_database()
    return db.execute_query(query)
