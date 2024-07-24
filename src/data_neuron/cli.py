import click
from .ask_cmd.main import query
from .context_init_cmd.main import init_context


@click.command()
@click.option('--init', is_flag=True, help='Initialize the context.')
@click.option('--ask', help='The question you want to ask about the database.')
def main(init, ask):
    if init:
        init_context()
    elif ask:
        query(ask)
    else:
        click.echo(main.get_help(click.get_current_context()))


if __name__ == '__main__':
    main()
