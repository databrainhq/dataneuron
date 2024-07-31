from .query_executor import execute_query
from .db_operations.factory import DatabaseFactory
import click
from .utils.print import print_success, print_info, create_box, print_error
from .core.sql_validator import sanitize_sql_query
from tabulate import tabulate
from threading import Lock
import time


def db_query_worker(sql_queue, state):
    context = state['context']
    processing_lock = Lock()
    try:
        while True:
            sql = sql_queue.get()
            if sql is None:  # Exit signal
                break

            with processing_lock:
                if state.get('processing'):
                    # Wait a bit to avoid immediate re-processing
                    time.sleep(0.1)
                    continue

                state['processing'] = True

                try:
                    db = DatabaseFactory.get_database()
                    result, column_names = db.execute_query_with_column_names(
                        sql)
                    state['sql_query'] = sql
                    state['db_result'] = (result, column_names)
                    print_success("\nQuery executed successfully!")
                except Exception as e:
                    print_error(f"\nError executing query: {str(e)}")
                    state['db_result'] = (str(e), None)
                finally:
                    state['processing'] = False
                    state['execution_complete'].set()
    except Exception as e:
        print_error(f"Unexpected error in db_query_worker: {str(e)}")
    finally:
        state['execution_complete'].set()
