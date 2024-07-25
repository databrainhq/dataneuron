import click
from .ask_cmd.main import query
from .context_init_cmd.main import init_context
from .db_init_cmd.main import init_database_config
from .chat_cmd.main import start_chat  # New import for chat functionality

VERSION = "0.1.1"  # Update this as you release new versions


@click.command()
@click.option('--init', is_flag=True, help='Initialize the context.')
@click.option('--ask', help='The question you want to ask about the database.')
@click.option('--db-init', type=click.Choice(['sqlite', 'mysql', 'mssql', 'postgres']), help='Initialize database configuration.')
# New option for chat
@click.option('--chat', is_flag=True, help='Start an interactive chat session.')
@click.option('--version', is_flag=True, help='Show the version of the tool')
def cli(init, ask, db_init, chat, version):
    if init:
        init_context()
    elif ask:
        query(ask)
    elif db_init:
        init_database_config(db_init)
    elif chat:
        start_chat()  # New function call for chat functionality
    elif version:
        click.echo(f"Data Neuron CLI version {VERSION}")
        return
    else:
        click.echo(cli.get_help(click.get_current_context()))


if __name__ == '__main__':
    cli()
