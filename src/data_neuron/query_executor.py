import sqlite3


def execute_query(query: str) -> str:
    try:
        conn = sqlite3.connect('database/sqlite.db')
        cursor = conn.cursor()

        cursor.execute(query)
        results = cursor.fetchall()

        # Format results as a string
        result_str = "\n".join([str(row) for row in results])

        return result_str
    except sqlite3.Error as e:
        return f"An error occurred: {str(e)}"
    except Exception as e:
        return f"An unexpected error occurred: {str(e)}"
    finally:
        if 'conn' in locals():
            conn.close()
