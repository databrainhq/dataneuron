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
    1. Determine if the question can potentially be answered using the given database structure, considering both the context and chat history if it exists.
    2. If it potentially can be answered:
       a. Identify any terms that might correspond to schema names, table names, column names, or data values.
       b. Replace any ambiguous or colloquial terms with their corresponding database terms.
       c. Resolve any multi-word phrases that might represent a single entity in the database.
       d. Provide a list of changes made to the original query.
       e. Identify specific entities (column values) that need to be validated against the full database.
       f. ALWAYS use "containing" for potential matches, even if the user's query is specific. For example, "email of Linda" should be phrased as "email of customer with name containing Linda".
       g. If there are no specific column values to validate, return an empty array for entities.
       h. Always use fully qualified table names (schema.table) in your refined query and explanations.
    3. If it cannot be answered based on the database structure, provide a clear explanation why.

    Important: 
    - The sample data provided is only a small subset (4-5 records) of the full dataset. It is given to help you understand the data structure and content types, not to determine the presence or absence of specific data.
    - Assume the full dataset may contain additional records not shown in the sample.
    - ALWAYS use "containing" or similar inclusive language for all potential matches, regardless of how specific the user's query is.

    When refining queries or suggesting potential matches:
    - Do not state definitively that data exists or doesn't exist based solely on the sample.
    - Use language that acknowledges the limited nature of the sample, such as "potentially exists", "might be found", or "could be present in the full dataset".
    - Suggest validations against the full dataset for any specific entities or values.
    - Always broaden specific queries to use "containing" to account for potential variations or partial matches.

    Return your response in the following JSON format:
    {{
        "can_be_answered": true/false,
        "explanation": "Your explanation here, acknowledging the limited sample data and using 'containing' for potential matches",
        "refined_query": "Your refined question here, using 'containing' for all potential matches",
        "changes": [
            "Change 1: Replaced [specific term] with '[term] containing [value]'",
            "Change 2",
            ...
        ],
        "entities": [
            {{
                "table": "schema.table_name",
                "column": "column_name",
                "potential_value": "value to check in full dataset, using 'containing'"
            }},
            ...
        ]
    }}

    Provide your response strictly in this JSON structure without any additional text.
    """
