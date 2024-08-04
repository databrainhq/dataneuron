def query_refinement_prompt(formatted_context: str, sample_data: str, user_query: str, chat_history: str) -> str:
    return f"""
    Given the following database context and sample data:

    {formatted_context}

    {sample_data}

    The user has asked the following question:
    "{user_query}"

    Chat History:
    {chat_history}


        Your task is to:
        1. Determine if the question can be answered using the given structure and also based on chat history if it exists.
        1a. If it cannot be answered, explain why.
        2. If it can be answered:
           a. Identify any terms that might correspond to schema names, table names, column names, or data values.
           b. Replace any ambiguous or colloquial terms with their corresponding database terms.
           c. Resolve any multi-word phrases that might represent a single entity in the database.
           d. Provide a list of changes made to the original query.
           e. Identify specific entities (column values) that need to be validated against the database.
           f. Use phrases like "containing", for potential matches.
           g. If there are no specific column values to validate, return an empty array for entities.
           h. Always use fully qualified table names (schema.table) in your refined query and explanations.
        3. If it cannot be answered, provide a clear explanation why.

        Return your response in the following JSON format:
        {{
            "can_be_answered": true/false,
            "explanation": "Your explanation here",
            "refined_query": "Your refined question here (if applicable)",
            "changes": [
                "Change 1",
                "Change 2",
                ...
            ],
            "entities": [
                {{
                    "table": "schema.table_name",
                    "column": "column_name",
                    "potential_value": "value to check"
                }},
                ...
            ]
        }}

        Please provide your response in this JSON structure. Strictly json no other text.
    """
