import datetime
import json
from ..utils.file_utils import format_yaml_for_prompt
from ..utils.date_functions import date_functions


def get_date_format_functions(database):
    if database == "mysql":
        return "DATE_FORMAT, QUARTER(), YEAR()... etc"
    else:
        return "TO_CHAR"


def get_json_extract_functions(database):
    if database == "postgres":
        return "json_extract_path_text, jsonb_each_text"
    elif database == "mysql":
        return "JSON_EXTRACT, JSON_UNQUOTE"
    elif database == "mssql":
        return "JSON_VALUE, JSON_QUERY"
    else:
        return "json extract function"


def get_date_functions(db_name):
    if db_name in [
        "postgres",
    ]:
        db_name = "postgres"

    if db_name in date_functions:
        return json.dumps(date_functions[db_name], indent=2)
    else:
        return f"Apply the appropiate date function based on selected database."


def get_db_rules(db):
    if db.lower() in [
        "postgres",
        "mssql",
        "sqlite"
    ]:
        """
        - Keywords like SELECT, INSERT, UPDATE, DELETE, etc., are case-insensitive; conventionally written in uppercase for better readability.
        - Strings in SQL should be denoted by single quotes. Special characters like the single quote ('), backslash (\), etc., within strings should be properly escaped.
        - Identifiers, including schema names, table names, and column names, are not case-sensitive; however, backticks (`) are used for enclosing identifiers that contain special characters or are MySQL reserved keywords.
        - If identifiers, including schema names, table names, and column names, contain spaces, DO NOT add underscore but should be enclosed in backticks (`).
        - Queries should consist of clauses (WITH, SELECT, FROM, WHERE, GROUP BY, HAVING, ORDER BY) in a specific order.
        - JOIN operations require aliases and should be written with explicit JOIN types (INNER JOIN, LEFT JOIN, etc.).
        - In SELECT statements, refrain from including schema names with table names; however, in the FROM section, ensure to use schema names along with table names. Identifiers should be specified with parent notation, e.g., `schema`.`table` as `table_alias`, `table_alias`.`column_name` as `column_alias`.
            e.g.: select `table_alias`.`column_name` as `column_alias` from `schema_name`.`table_name` as `table_alias`.
        - Sql Query should use aliases in lower case. Use appropriate aliases for columns, tables, joins, sub queries and CTEs with the "AS" keyword. Alias should be enclosed in in backticks (`).
        """

    if db.lower() in ["mysql"]:
        """
        - Strings in SQL should be denoted by single quotes. Special characters like the single quote ('), backslash (\), etc., within strings should be properly escaped.
        - Identifiers, including schema names, table names, and column names, should be enclosed in double quotes.
        - if identifiers, including schema names, table names, and column names, contain spaces, DO NOT add underscore but should be enclosed in double quotes.
        - Queries should consist of clauses (WITH, SELECT, FROM, WHERE, GROUP BY, HAVING, ORDER BY) in a specific order.
        - JOIN operations require aliases and should be written with explicit JOIN types (INNER JOIN, LEFT JOIN, etc.).
        - In SELECT statements, refrain from including schema names with table names; however, in the FROM section, ensure to use schema names along with table names. Identifiers should be specified with parent notation, e.g., "schema"."table" as "table_alias", "table_alias"."column_name" as "column_alias".
            e.g.: select "table_alias"."column_name" as "column_alias" from "schema_name"."table_name" as "table_alias".
        - Sql Query should use aliases in lower case. Use appropriate aliases for columns, tables, joins, sub queries and CTEs with the "AS" keyword. Alias should be enclosed in double quotes.
        """


def sql_query_prompt(query, context):
    db_config = context["database"]
    db = db_config.get("name")
    context_prompt = "Database Context:\n\n"

    # Format table information
    context_prompt += "Tables:\n"
    for table_name, table_data in context["tables"].items():
        context_prompt += f"  {table_name}:\n"
        context_prompt += format_yaml_for_prompt(
            table_data).replace("\n", "\n    ")
        context_prompt += "\n"

    # Format relationships
    context_prompt += "\nRelationships:\n"
    context_prompt += format_yaml_for_prompt(context["relationships"]).replace(
        "\n", "\n  "
    )

    # Format global definitions
    context_prompt += "\nGlobal Definitions:\n"
    context_prompt += format_yaml_for_prompt(context["global_definitions"]).replace(
        "\n", "\n  "
    )

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
    - Based on the query, formulate an SQL query targeting the database: "{db}".
    - Follow best practices for SQL query construction, including proper syntax formatting, and strictly adhere to the SQL rules provided below:
        {get_db_rules(db)}
        - Ensure to apply appropriate filters based on the column type, and if necessary, cast the column to the correct data type for accurate querying.
        - Prefer to use appropriate date functions to filter date columns instead of static date filters. If necessary, cast the column to date to ensure accurate filtering.
        - If you encounter any JSON format column, use appropriate functions({get_json_extract_functions(db)}) for JSON extraction based on the database.
        - Current Date: {datetime.datetime.now().strftime('%Y-%m-%d')}
        - Available Date SQL Functions(Only use date functions from the provided list):
            {get_date_functions(db)}

    Please format your response as an XML as follows:

    example:



    <response>
        <sql> SELECT COUNT(id) FROM users </sql>
        <explanation>
            Counts the total number of users in the users table.
        </explanation>
        <references>
            Tables: users
            Columns: users.id
        </references>
        <note>
            Assumes the users table has a unique id for each user.
        </note>
    </response>

    Follow the XML format strictly. Answer with only XML response as it will be parsed as xml, no other extra words or formatting.
    """

    return prompt
