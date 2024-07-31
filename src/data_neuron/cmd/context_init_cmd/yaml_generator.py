from ...prompts.yaml_generation_prompt import definitions_relationships_prompt
from ...api.main import stream_neuron_api


def generate_definitions_and_relationships(tables, db_type):
    # Check if tables is a list of dictionaries or a list of strings
    if tables and isinstance(tables[0], dict):
        table_names = [
            f"{table['schema']}.{table['table']}" for table in tables]
    elif tables and isinstance(tables[0], str):
        table_names = tables
    else:
        raise ValueError("Unexpected table format")

    prompt = definitions_relationships_prompt(table_names, db_type)
    system_prompt = "You are a helpful assistant that generates YAML content for database definitions and relationships."

    yaml_content = ""
    for chunk in stream_neuron_api(prompt, instruction_prompt=system_prompt):
        yaml_content += chunk

    # Split the YAML content into definitions and relationships
    definitions_yaml, relationships_yaml = yaml_content.split('---')
    return definitions_yaml.strip(), relationships_yaml.strip()
