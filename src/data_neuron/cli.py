import click
from .cmd.ask_cmd import query
# from .cmd.context_init_cmd.main import init_context
# from .cmd.db_init_cmd.main import init_database_config
# from .cmd.chat_cmd.main import start_chat  # New import for chat functionality
# from .cmd.report_cmd.main import generate_report  # New import
# from .server import run_server

VERSION = "0.2.2"  # Update this as you release new versions


@click.command()
@click.option('--init', is_flag=True, help='Initialize the context.')
# @click.option('--ask', help='The question you want to ask about the database.')
@click.option('--db-init', type=click.Choice(['sqlite', 'mysql', 'mssql', 'postgres', 'csv', 'clickhouse']), help='Initialize database configuration.')
# New option for chat
@click.option('--chat', help='Start an interactive chat session.', type=str)
@click.option('--version', is_flag=True, help='Show the version of the tool')
# New option
@click.option('--report', is_flag=True, help='Generate a dashboard report.')
@click.option('--ask', help='Start the Flask API server.')
@click.option('--server', is_flag=True, help='Start the Flask API server.')
@click.option('--prod', is_flag=True, help='Run the server in production mode.')
@click.option('--host', default='0.0.0.0', help='Host to run the server on')
@click.option('--port', type=int, default=8040, help='Port to run the server on')
def cli(init, db_init, chat, version, report, ask, server, prod, host, port):
    # if init:
    #     init_context()
    # elif ask:
    query(ask)
    # elif db_init:
    #     init_database_config(db_init)
    # elif chat:
    #     start_chat(chat)  # New function call for chat functionality
    # elif version:
    #     click.echo(f"Data Neuron CLI version {VERSION}")
    #     return
    # elif report:
    #     generate_report()
    # elif server:
    #     debug = not prod
    #     if prod:
    #         os.environ['FLASK_ENV'] = 'production'
    #     run_server(host=host, port=port, debug=debug)
    # else:
    #     click.echo(cli.get_help(click.get_current_context()))


if __name__ == '__main__':
    cli()
