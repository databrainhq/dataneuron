import click
import os
import yaml
from ..prompts.sql_query_prompt import sql_query_prompt
from ..utils.stream_print import process_simplified_xml
import threading
from queue import Queue
from ..ask_cmd.query_worker import db_query_worker
from ..query_refiner import process_query
from ..api.main import stream_neuron_api
from ..context_loader import load_context
from ..utils.print import print_header, print_prompt, print_info, print_success, print_warning

MAX_CHAT_HISTORY = 5  # Maximum number of messages to keep in chat history


def display_options():
    click.echo(click.style("\nAvailable commands:", fg="cyan"))
    click.echo(click.style("  query - Enter your question", fg="cyan"))
    click.echo(click.style("  save (s) - Save last query as a metric", fg="cyan"))
    click.echo(click.style("  exit/quit/bye - End the session", fg="cyan"))


def styled_prompt(message):
    return click.prompt(click.style(f"\n{message}", fg="green", bold=True))


def list_dashboards():
    dashboards_dir = "dashboards"
    if not os.path.exists(dashboards_dir):
        return []
    return [f.split('.')[0] for f in os.listdir(dashboards_dir) if f.endswith('.yml')]


def start_chat():
    print_header("Starting DATA neuron chat session...")
    print_warning(
        "Please remember your messages and result wll be sent to LLM for API call.")
    print_info("🗄️ Fetching the context from your context folder\n")
    context = load_context()
    print_success("Context is loaded!\n")

    chat_history = []
    last_query = {}

    while True:
        display_options()
        user_input = styled_prompt("You:> ")

        if user_input.lower() in ['exit', 'quit', 'bye']:
            print_info("Ending chat session. Goodbye!")
            break

        if user_input.lower() in ['save', 's']:
            if last_query:
                save_metric_to_dashboard(last_query)
            else:
                print_warning("No query to save. Please run a query first.")
            continue

        sql_query, db_result, changed_query = process_with_llm(
            user_input, context, chat_history)

        last_query = {
            "original_query": user_input,
            "changed_query": changed_query,
            "sql_query": sql_query,
            "result": db_result
        }

        assistant_response = f"generated_sql: {sql_query}, result: {db_result}"
        chat_history.append({"role": "user", "content": user_input})
        chat_history.append(
            {"role": "assistant", "content": assistant_response})

        if len(chat_history) > MAX_CHAT_HISTORY * 2:
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

    db_thread = threading.Thread(
        target=db_query_worker, args=(state['sql_queue'], state))
    db_thread.start()

    assistant_response = ""
    for chunk in stream_neuron_api(prompt, chat_history, system_prompt):
        process_simplified_xml(chunk, state)
        assistant_response += chunk

    sql_query = state['sql_query']
    state['sql_queue'].put(None)
    db_result = state['db_result']
    db_thread.join()

    print("\n")
    return sql_query, db_result, changed_query


def save_metric_to_dashboard(query):
    existing_dashboards = list_dashboards()

    if existing_dashboards:
        click.echo(click.style("\nExisting dashboards:", fg="cyan"))
        for idx, dashboard in enumerate(existing_dashboards, 1):
            click.echo(click.style(f"  {idx}. {dashboard}", fg="cyan"))
        click.echo(click.style("  0. Create a new dashboard", fg="cyan"))

        choice = styled_prompt(
            "Choose a dashboard (number) or enter a new name")

        if choice.isdigit():
            choice = int(choice)
            if 1 <= choice <= len(existing_dashboards):
                dashboard_name = existing_dashboards[choice - 1]
            elif choice == 0:
                dashboard_name = styled_prompt("Enter new dashboard name")
            else:
                print_warning("Invalid choice. Creating a new dashboard.")
                dashboard_name = styled_prompt("Enter new dashboard name")
        else:
            dashboard_name = choice
    else:
        dashboard_name = styled_prompt("Enter dashboard name")

    metric_name = styled_prompt("Enter metric name")

    dashboards_dir = "dashboards"
    os.makedirs(dashboards_dir, exist_ok=True)

    dashboard_file = os.path.join(dashboards_dir, f"{dashboard_name}.yml")

    if os.path.exists(dashboard_file):
        with open(dashboard_file, 'r') as f:
            dashboard = yaml.safe_load(f)
    else:
        dashboard = {
            "name": dashboard_name,
            "description": styled_prompt("Enter dashboard description"),
            "metrics": []
        }

    metric = {
        "name": metric_name,
        "original_query": query["original_query"],
        "changed_query": query["changed_query"],
        "sql_query": query["sql_query"]
    }

    dashboard["metrics"].append(metric)

    with open(dashboard_file, 'w') as f:
        yaml.dump(dashboard, f, default_flow_style=False)

    print_success(
        f"Metric '{metric_name}' saved to dashboard '{dashboard_name}'")


if __name__ == '__main__':
    start_chat()
