import os
import yaml
from ..utils.print import print_info, print_success, print_warning, styled_prompt
import click


def mark_client_tables():
    print_info("Setting up client information...")
    context_name = click.prompt("Enter the context name", type=str)
    context_dir = os.path.join('context', context_name)
    if not os.path.exists(context_dir):
        print_warning(
            f"Context '{context_name}' does not exist. Please create it first using 'dnn --init'.")
        return

    client_info_path = os.path.join(context_dir, 'client_info.yaml')

    # Load existing client info if it exists
    if os.path.exists(client_info_path):
        with open(client_info_path, 'r') as f:
            client_info = yaml.safe_load(f)
    else:
        client_info = {'schemas': set(), 'tables': {}}

    # Convert schemas to a set if it's a list
    if isinstance(client_info['schemas'], list):
        client_info['schemas'] = set(client_info['schemas'])

    # Prompt for client tables
    while True:
        table_name = styled_prompt(
            "Enter a table name for client filtering of the format: schema.tablename (e.g., main.orders) or 'done' to finish")
        if table_name.lower() == 'done':
            break

        # Split the table name into schema and table
        parts = table_name.split('.')
        if len(parts) != 2:
            print_warning(
                "Invalid table name format. Please use 'schema.tablename'.")
            continue

        schema, table = parts
        client_info['schemas'].add(schema)

        client_id_column = styled_prompt(
            f"Enter the client ID column name for {table_name}")
        client_info['tables'][table_name] = client_id_column

    # Convert schemas set to list for YAML serialization
    client_info['schemas'] = list(client_info['schemas'])

    # Save client_info to yaml file
    with open(client_info_path, 'w') as f:
        yaml.dump(client_info, f)

    print_success(f"Client information updated in {client_info_path}")
