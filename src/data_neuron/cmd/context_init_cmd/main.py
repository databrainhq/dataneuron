import os
import click
from .table_operations import get_table_list, choose_tables, generate_yaml_for_table
from .yaml_generator import generate_definitions_and_relationships
from ...utils.print import print_header, print_info, print_success, print_warning, print_prompt
from ...db_operations.error_handler import handle_database_errors


@handle_database_errors
def init_context():
    print_header(
        "Setting up a new context(semantic layer) for your database..")

    context_name = click.prompt(
        "Please enter a name for this context eg: customer_success or product_analytics", type=str)
    context_dir = os.path.join('context', context_name)

    if os.path.exists(context_dir):
        overwrite = click.confirm(
            f"Context '{context_name}' already exists. Do you want to overwrite it?", default=False)
        if not overwrite:
            print_info("Context initialization cancelled.")
            return

    print_warning(
        f"This will create a folder {context_dir} in the current directory. Existing files will be overwritten.")
    print_prompt("You can edit it anytime...")
    print_warning("Please choose a set of 10 or lesser tables..\n")
    print_info("🗄️ Fetching tables from the database")
    db_type, all_tables = get_table_list()
    chosen_tables = choose_tables(all_tables)

    os.makedirs(os.path.join(context_dir, 'tables'), exist_ok=True)

    for table_info in chosen_tables:
        if isinstance(table_info, dict):
            schema = table_info['schema']
            table = table_info['table']
        else:
            schema = 'main'  # default schema
            table = table_info

        yaml_content = generate_yaml_for_table(schema, table)

        with open(os.path.join(context_dir, 'tables', f'{schema}__{table}.yaml'), 'w') as f:
            f.write(yaml_content)

        print("\n")
        print_success(f"Generated YAML for table: {schema}.{table}")

    print_info("Generating definitions and relationships...")
    try:
        definitions_yaml, relationships_yaml = generate_definitions_and_relationships(
            chosen_tables, db_type)

        with open(os.path.join(context_dir, 'definitions.yaml'), 'w') as f:
            f.write(definitions_yaml)
        print_success("Generated definitions.yaml")

        with open(os.path.join(context_dir, 'relationships.yaml'), 'w') as f:
            f.write(relationships_yaml)
        print_success("Generated relationships.yaml")

        print_success(f"Initialization complete for context: {context_name}")
    except Exception as e:
        print_warning(f"An error occurred: {str(e)}")


if __name__ == '__main__':
    init_context()
