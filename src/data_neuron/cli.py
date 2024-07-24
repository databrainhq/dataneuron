import click
from .ask_cmd.main import query


@click.command()
@click.option('--ask', prompt='Your question', help='The question you want to ask about the database.')
def main(ask):
    query(ask)


if __name__ == '__main__':
    main()
