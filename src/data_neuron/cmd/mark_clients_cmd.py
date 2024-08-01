import os
import yaml
from ..utils.print import print_success
import click


def mark_client_tables():
    context_name = click.prompt("Enter the context name", type=str)
    client_tables = {}
    while True:
        table_name = click.prompt(
            "Enter schema name and table name eg: main.orders (or 'done' to finish)", type=str)
        if table_name.lower() == 'done':
            break
        client_id_column = click.prompt(
            f"Enter client ID column name for {table_name}", type=str)
        client_tables[table_name] = client_id_column
    context_dir = os.path.join('context', context_name)
    client_tables_path = os.path.join(context_dir, 'client_tables.yaml')

    with open(client_tables_path, 'w') as f:
        yaml.dump(client_tables, f)

    print_success(f"Marked {len(client_tables)} tables with client ID columns")
