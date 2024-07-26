
import click
from ..db_operations.factory import DatabaseFactory


def get_table_list():
    db = DatabaseFactory.get_database()
    res = db.get_table_list()
    return db.db_type, res


def choose_tables(all_tables):
    chosen_tables = []
    for i in range(0, len(all_tables), 10):
        batch = all_tables[i:i+10]
        click.echo(
            "Choose tables (enter numbers separated by commas, or 'all' for all, 'skip' for next batch, 'done' to finish):")
        for idx, table in enumerate(batch, start=1):
            click.echo(f"{idx}. {table}")
        choice = click.prompt("Your choice").lower()
        if choice == 'all':
            chosen_tables.extend(batch)
            break
        elif choice == 'skip':
            continue
        elif choice == 'done':
            break
        else:
            chosen_tables.extend([batch[int(i)-1] for i in choice.split(',')
                                 if i.strip().isdigit() and 0 < int(i) <= len(batch)])
    return chosen_tables


def generate_yaml_for_table(table_name):
    db = DatabaseFactory.get_database()
    table_info = db.get_table_info(table_name)

    from ..prompts.yaml_generation_prompt import table_yaml_prompt
    from ..api.main import stream_neuron_api

    prompt = table_yaml_prompt(table_info, db.db_type)
    system_prompt = "You are a helpful assistant that generates YAML content based on database table information."

    yaml_content = ""
    for chunk in stream_neuron_api(prompt, instruction_prompt=system_prompt):
        click.echo(chunk, nl=False)
        yaml_content += chunk

    return yaml_content
