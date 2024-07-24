from ..utils.file_utils import format_yaml_for_prompt


def sql_query_prompt(query, context):
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

    return prompt
