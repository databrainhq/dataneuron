import click
import os
from .context_loader import load_context
from .llm_processor import process_with_llm
from .query_executor import execute_query


@click.command()
@click.option('--ask', prompt='Your question', help='The question you want to ask about the database.')
def main(ask):
    """Data Neuron CLI tool for querying your database using natural language."""

    # Check if we're in the correct directory structure
    if not os.path.exists('database/sqlite.db') or not os.path.exists('context'):
        click.echo(
            "Error: Make sure you're in the correct directory with 'database/sqlite.db' and 'context' folder.")
        return

    # Load context
    context = load_context()

    # Process the question with LLM
    sql_query = process_with_llm(ask, context)

    # Execute the query
    result = execute_query(sql_query)

    # Display the result
    click.echo(f"Query: {sql_query}")
    click.echo("Result:")
    click.echo(result)


if __name__ == '__main__':
    main()
