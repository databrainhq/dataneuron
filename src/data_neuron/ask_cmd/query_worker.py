from ..query_executor import execute_query
import click
from ..utils.print import print_success, print_info


def db_query_worker(sql_queue, state):
    while True:
        sql = sql_queue.get()
        if sql is None:  # Exit signal
            break
        try:
            result = execute_query(sql)
            state['db_result'] = result
            print_success("\nQuery executed successfully!")
            click.echo(click.style("Result:", fg="green", bold=True))
            click.echo(result)
        except Exception as e:
            print_info(f"\nError executing query: {str(e)}")
