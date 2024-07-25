import click
from ..prompts.sql_query_prompt import sql_query_prompt
from ..utils.stream_print import process_simplified_xml
import threading
from queue import Queue
from ..ask_cmd.query_worker import db_query_worker
from ..query_refiner import process_query
from ..api.main import stream_neuron_api
from ..context_loader import load_context
from ..utils.print import print_header, print_prompt, print_info, print_success

MAX_CHAT_HISTORY = 5  # Maximum number of messages to keep in chat history


def start_chat():
    print_header("Starting DATA neuron chat session...")
    print_info("🗄️ Fetching the context from your context folder\n")
    print_prompt("You can exit anytime by entering exit or quit or bye\n")
    context = load_context()
    print_success("Context is loaded!\n")

    chat_history = []

    while True:
        user_input = click.prompt("You:> ", prompt_suffix="\n", type=str)

        if user_input.lower() in ['exit', 'quit', 'bye']:
            print_info("Ending chat session. Goodbye!")
            break

        assistant_response = process_with_llm(
            user_input, context, chat_history)
        chat_history.append({"role": "user", "content": user_input})
        chat_history.append(
            {"role": "assistant", "content": assistant_response})

        # Trim chat history if it exceeds the maximum length
        if len(chat_history) > MAX_CHAT_HISTORY * 2:  # *2 because each Q&A is two entries
            chat_history = chat_history[-MAX_CHAT_HISTORY * 2:]


def process_with_llm(query: str, context: dict, chat_history: list):
    print_header("DATA neuron is thinking...")
    print_prompt("🤖 Sending request to LLM\n")

    print_info("Rephrasing the question suited for db")
    changed_query = process_query(query)
    prompt = sql_query_prompt(changed_query, context)
    system_prompt = "You are a helpful assistant that generates SQL queries based on natural language questions and maintains context throughout the conversation."

    state = {
        'buffer': '',
        'sql_queue': Queue(),
        'db_result': None,
        'sql_query': None,
        'context': context
    }

    # Start a thread for database querying
    db_thread = threading.Thread(
        target=db_query_worker, args=(state['sql_queue'], state))
    db_thread.start()

    assistant_response = ""
    for chunk in stream_neuron_api(prompt, chat_history, system_prompt):
        process_simplified_xml(chunk, state)
        assistant_response += chunk

    sql_query = state['sql_query']
    state['sql_queue'].put(None)
    # Signal the database thread to finish
    db_result = state['db_result']

    db_thread.join()

    print("\n")  # Add a newline for better readability
    return f"generated_sql: {sql_query}, result: {db_result}"


if __name__ == '__main__':
    start_chat()
