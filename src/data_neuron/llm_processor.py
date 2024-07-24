import openai
import yaml
from .config import OPENAI_API_KEY

openai.api_key = OPENAI_API_KEY


def format_yaml_for_prompt(yaml_data):
    return yaml.dump(yaml_data, default_flow_style=False)


def process_with_llm(query: str, context: dict) -> str:
    context_prompt = "Database Context:\n\n"

    # Format table information
    context_prompt += "Tables:\n"
    for table_name, table_data in context['tables'].items():
        context_prompt += f"  {table_name}:\n"
        context_prompt += format_yaml_for_prompt(
            table_data).replace('\n', '\n    ')
        context_prompt += "\n"

    # Format relationships
    context_prompt += "\nRelationships:\n"
    context_prompt += format_yaml_for_prompt(
        context['relationships']).replace('\n', '\n  ')

    # Format global definitions
    context_prompt += "\nGlobal Definitions:\n"
    context_prompt += format_yaml_for_prompt(
        context['global_definitions']).replace('\n', '\n  ')

    prompt = f"""
    {context_prompt}
    
    User Query: "{query}"
    
    Please generate an SQL query to answer the user's question.
    Use the provided aliases and global definitions to interpret the user's query.
    Return only the SQL query, without any additional explanation.
    """

    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": "You are a helpful assistant that generates SQL queries based on natural language questions."},
            {"role": "user", "content": prompt}
        ]
    )

    return response.choices[0].message['content'].strip()
