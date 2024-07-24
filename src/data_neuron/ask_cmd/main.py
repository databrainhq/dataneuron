import click
import os
from ..context_loader import load_context
from ..llm_processor import process_with_llm
from ..query_executor import execute_query
from ..utils.print import print_header, print_prompt, print_info, print_success, print_info_secondary, create_box


def query(ask):

    print_header("DATA neuron is thinking..")
    if not os.path.exists('database/sqlite.db') or not os.path.exists('context'):
        click.echo(
            "Error: Make sure you're in the correct directory with 'database/sqlite.db' and 'context' folder.")
        return

    print_info("🗄️ Fetching the context from your context folder")
    # Load context
    context = load_context()

    print_success("Context is loaded!\n")

    print_prompt("🤖 Sending request to LLM\n")
    # # # Process the question with LLM
    sql_query = process_with_llm(ask, context)

    print_info_secondary(f"SQL query: {sql_query}\n")

    # # print_info("⛏️ Executing the query..")
    # # # Execute the query
    # # result = execute_query(sql_query)
    # # result = "30"

    # res = create_box("Result", result, "")
    # print(res)


if __name__ == '__main__':
    query()
