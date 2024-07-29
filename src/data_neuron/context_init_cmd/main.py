import os
from .table_operations import get_table_list, choose_tables, generate_yaml_for_table
from .yaml_generator import generate_definitions_and_relationships
from ..utils.print import print_header, print_info, print_success, print_warning, print_prompt
from ..db_operations.error_handler import handle_database_errors


@handle_database_errors
def init_context():
    print_header("Setting up the context(semantic layer) for your database..")
    print_warning(
        "This will create a folder context in the current directory. And override if there is an existing file")
    print_prompt("You can edit it anytime...")
    print_warning("Please choose a set of 10 or lesser tables..\n")
    print_info("🗄️ Fetching tables from the database")

    db_type, all_tables = get_table_list()

    chosen_tables = choose_tables(all_tables)

    os.makedirs('context/tables', exist_ok=True)

    for table_info in chosen_tables:
        if isinstance(table_info, dict):
            schema = table_info['schema']
            table = table_info['table']
        else:
            schema = 'main'  # default schema
            table = table_info

        yaml_content = generate_yaml_for_table(schema, table)
        with open(f'context/tables/{schema}___{table}.yaml', 'w') as f:
            f.write(yaml_content)
        print("\n")
        print_success(f"Generated YAML for table: {schema}.{table}")

    print_info("Generating definitions and relationships...")
    try:
        definitions_yaml, relationships_yaml = generate_definitions_and_relationships(
            chosen_tables, db_type)

        with open('context/definitions.yaml', 'w') as f:
            f.write(definitions_yaml)
        print_success("Generated definitions.yaml")

        with open('context/relationships.yaml', 'w') as f:
            f.write(relationships_yaml)
        print_success("Generated relationships.yaml")

        print_success("Initialization complete!")
    except Exception as e:
        print_warning(f"An error occurred: {str(e)}")


if __name__ == '__main__':
    init_context()
