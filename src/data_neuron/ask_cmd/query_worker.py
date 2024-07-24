from ..query_executor import execute_query
from ..db_operations.factory import DatabaseFactory
import click
from ..utils.print import print_success, print_info

# to parallely query the db as the stream from llm is happening
# once <sql>..</sql> tag is processed the sq_queue will be updated.


def db_query_worker(sql_queue, state):
    while True:
        sql = sql_queue.get()
        if sql is None:  # Exit signal
            break
        try:
            db = DatabaseFactory.get_database()
            result = db.execute_query(sql)
            state['sql_query'] = sql
            state['db_result'] = result
            print_success("\nQuery executed successfully!")
            click.echo(click.style("Result:", fg="green", bold=True))
            click.echo(result)
        except Exception as e:
            print_info(f"\nError executing query: {str(e)}")
