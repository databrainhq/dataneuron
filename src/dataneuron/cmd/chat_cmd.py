# src/dataneuron/cmd/chat_cmd.py

import click
from ..core.data_neuron import DataNeuron
from ..core.dashboard_manager import DashboardManager
from ..utils.print import print_info, print_success, print_prompt, print_warning


def chat_cmd(context):
    """Start an interactive chat session with the database."""
    dn = DataNeuron(db_config='database.yaml', context=context, log=True)
    dn.initialize()
    dashboard_manager = DashboardManager()

    print_info(
        "Starting chat session. You can start chatting with your data. Type 'exit' to end the conversation.")
    print_info("Other Available commands:")
    print_info(
        "  /save <dashboard_name> - Save the last query to a dashboard")
    print_info("  /list - List all available dashboards")
    print_info(
        "  /view <dashboard_name> - View the contents of a dashboard")

    last_query = None
    while True:
        user_input = click.prompt("You", prompt_suffix="> ")

        if user_input.lower() in ['exit', 'quit', 'bye']:
            print_info("Ending chat session. Goodbye!")
            break

        if user_input.startswith('/'):
            handle_special_command(user_input, last_query, dashboard_manager)
            continue

        sql, response = dn.chat(user_input)
        print()  # Add a blank line for better readability

        # Extract the SQL query from the response
        last_query = sql


def handle_special_command(command, last_query, dashboard_manager):
    parts = command.split()
    if parts[0] == '/save':
        if len(parts) < 2:
            print_warning("Please provide a dashboard name.")
            return
        if not last_query:
            print_warning("No query to save. Run a query first.")
            return
        dashboard_name = ' '.join(parts[1:])
        metric_name = click.prompt("Enter a name for this metric")
        dashboard_manager.save_to_dashboard(
            dashboard_name, metric_name, last_query)
        print_success(
            f"Query saved to dashboard '{dashboard_name}' as metric '{metric_name}'")
    elif parts[0] == '/list':
        dashboards = dashboard_manager.list_dashboards()
        if dashboards:
            print_info("Available dashboards:")
            for dashboard in dashboards:
                print_info(f"  - {dashboard}")
        else:
            print_info("No dashboards available.")
    elif parts[0] == '/view':
        if len(parts) < 2:
            print_warning("Please provide a dashboard name.")
            return
        dashboard_name = ' '.join(parts[1:])
        metrics = dashboard_manager.view_dashboard(dashboard_name)
        if metrics:
            print_info(f"Contents of dashboard '{dashboard_name}':")
            for idx, metric in enumerate(metrics, 1):
                print_info(f"  {idx}. {metric['name']}: {metric['sql_query']}")
        else:
            print_warning(
                f"Dashboard '{dashboard_name}' not found or is empty.")
    else:
        print_warning(f"Unknown command: {parts[0]}")
