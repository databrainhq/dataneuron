import sqlite3


def execute_query(query: str) -> str:
    conn = sqlite3.connect('database/sqlite.db')
    cursor = conn.cursor()

    try:
        cursor.execute(query)
        results = cursor.fetchall()

        # Format results as a string
        result_str = "\n".join([str(row) for row in results])

        return result_str
    except sqlite3.Error as e:
        return f"An error occurred: {e}"
    finally:
        conn.close()
