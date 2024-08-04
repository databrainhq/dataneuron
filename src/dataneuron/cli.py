import click
from .cmd.ask_cmd import query
from .cmd.db_init_cmd import init_database_config
from .cmd.chat_cmd import chat_cmd
from .cmd.report_cmd import generate_report  # New import
from .cmd.mark_clients_cmd import mark_client_tables
from .server import run_server
from .cmd.context_init_cmd import ContextInitializer

import os

VERSION = "0.3.1"  # Update this as you release new versions


@click.command()
@click.option('--init', is_flag=True, help='Initialize the context.')
@click.option('--db-init', type=click.Choice(['sqlite', 'mysql', 'mssql', 'postgres', 'csv', 'clickhouse']), help='Initialize database configuration.')
# New option for chat
@click.option('--chat', help='Start an interactive chat session.', type=str)
@click.option('--version', is_flag=True, help='Show the version of the tool')
# New option
@click.option('--report', is_flag=True, help='Generate a dashboard report.')
@click.option('--ask', help='Start the Flask API server.')
@click.option('--context', help='The context to ask questions on')
@click.option('--server', is_flag=True, help='Start the Flask API server.')
@click.option('--prod', is_flag=True, help='Run the server in production mode.')
@click.option('--host', default='0.0.0.0', help='Host to run the server on')
@click.option('--port', type=int, default=8040, help='Port to run the server on')
@click.option('--mc', is_flag=True, help='Mark tables with client ID columns.')
def cli(init, db_init, chat, version, report, context, ask, server, prod, host, port, mc):
    if init:
        initializer = ContextInitializer()
        initializer.init_context()
    elif ask:
        query(ask, context)
    elif db_init:
        init_database_config(db_init)
    elif chat:
        chat_cmd(chat)
    elif version:
        click.echo(f"Data Neuron CLI version {VERSION}")
        return
    elif report:
        generate_report()
    elif server:
        debug = not prod
        if prod:
            os.environ['FLASK_ENV'] = 'production'
        run_server(host=host, port=port, debug=debug)
    elif mc:
        mark_client_tables()
    else:
        click.echo(cli.get_help(click.get_current_context()))


if __name__ == '__main__':
    cli()
