from ..prompts.yaml_generation_prompt import definitions_relationships_prompt
from ..api.main import stream_neuron_api


def generate_definitions_and_relationships(tables, db_type):
    prompt = definitions_relationships_prompt(tables, db_type)
    system_prompt = "You are a helpful assistant that generates YAML content for database definitions and relationships."

    yaml_content = ""
    for chunk in stream_neuron_api(prompt, instruction_prompt=system_prompt):
        yaml_content += chunk

    definitions_yaml, relationships_yaml = yaml_content.split('---')
    return definitions_yaml.strip(), relationships_yaml.strip()
