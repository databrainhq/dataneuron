from ..prompts.sql_query_prompt import sql_query_prompt
from ..utils.stream_print import process_simplified_xml
import threading
from queue import Queue
from .query_worker import db_query_worker
from ..api.main import stream_neuron_api
import click
import os
from ..context_loader import load_context
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
    process_with_llm(ask, context)


def process_with_llm(query: str, context: dict) -> str:
    prompt = sql_query_prompt(query, context)
    system_prompt = "You are a helpful assistant that generates SQL queries based on natural language questions."
    xml_buffer = ""
    state = {
        'buffer': '',
        'in_step': False,
        'sql_queue': Queue(),
        'db_result': None
    }

    # Start a thread for database querying
    db_thread = threading.Thread(
        target=db_query_worker, args=(state['sql_queue'], state))
    db_thread.start()

    for chunk in stream_neuron_api(prompt, instruction_prompt=system_prompt):
        process_simplified_xml(chunk, state)
        xml_buffer += chunk

    # Signal the database thread to finish
    state['sql_queue'].put(None)
    db_thread.join()


if __name__ == '__main__':
    query()
