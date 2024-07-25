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
    
    The Query: "{query}"
    
    Based on the given database context and the given query, please provide:
    
    1. A very short explanation of your reasoning process, including:
       - How you interpreted the user's question
       - Which tables and columns you chose to use and why
       - Any assumptions you made
       - Any potential ambiguities in the query and how you resolved them
    2. A list of the specific tables, columns, and definitions you referenced from the provided context.
    3. The SQL query to answer the user's question.
    4. Any caveats or limitations of the generated SQL query.

    SQL query guidelines:
    - Only generate SELECT statements, non-write, non-destructive queries.
    - Only reference tables, coilumns given in the context.
    - Use the provided aliases and global definitions to interpret the user's query accurately.
    - Use actual columns and tables to query. Never use alias to query the column.
        example:
            context: users.id with alias uid
            correct: SELECT id as uid .. 
            incorrect: SELECT uid

    Please format your response as an XML as follows:

    example:

    <response>
        <sql> The generated SQL query </sql>
        <explanation> Your explanation </explanation>
        <references>
        Referenced Elements:
        - Tables: users, orders
        - Columns: users.name, orders.id
        - Definitions: ..
        </references>
        <note>
        Any caveats or limitations of the query
        </note>
    </response>


    Stricy answer with only XML response as it will be parsed as xml, no other extra words.
    """

    return prompt
