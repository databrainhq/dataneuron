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


def get_sql_rules(db):
    common_rules = """
    - Keywords are case-insensitive but conventionally written in uppercase.
    - Use single quotes for strings, escaping special characters properly.
    - Specify required columns explicitly instead of using SELECT *.
    - Use appropriate data types for comparisons and joins.
    - For large datasets, use LIMIT (or TOP in MSSQL) with ORDER BY for pagination.
    - Use EXISTS instead of IN for better performance when checking for related records.
    - Utilize window functions (ROW_NUMBER(), RANK(), LAG(), LEAD()) for advanced analytics.
    - Use IS NULL or IS NOT NULL for NULL value comparisons.
    - Optimize subqueries: prefer correlated subqueries for row-by-row processing and non-correlated for set-based operations.
    - Use CTEs for complex queries to improve query structure.
    """

    if db == "postgres":
        return common_rules + """
        - Identifiers are case-sensitive. Always enclose them in double quotes (").
        - Schema usage: PostgreSQL uses schemas, with "public" as the default schema.
          - If tables are in the public schema, you can omit the schema name.
          - For tables in other schemas, always specify the schema: "schema_name"."table_name"
        - Example: SELECT "table_alias"."column_name" AS "column_alias" FROM "schema_name"."table_name" AS "table_alias"
        - Use LATERAL joins for more efficient correlated subqueries.
        - Utilize GIN or GiST indexes for full-text search with tsvector and tsquery.
        - For JSON operations, use json_extract_path_text() or jsonb_each_text().
        - Use CURRENT_DATE, CURRENT_TIMESTAMP for current date and time.
        """
    elif db == "mysql":
        return common_rules + """
        - Identifiers are case-sensitive on Unix systems. Enclose them in backticks (`) if they contain spaces, special characters, or are reserved keywords.
        - Schema usage: In MySQL, each database is its own schema.
          - Do not use schema names like 'public' before table names.
          - If you need to specify a database, use: `database_name`.`table_name`
        - Example: SELECT `table_alias`.`column_name` AS `column_alias` FROM `table_name` AS `table_alias`
        - Use STRAIGHT_JOIN when you need to override the optimizer's join order.
        - For full-text search, use MATCH ... AGAINST with appropriate full-text indexes.
        - For JSON operations, use JSON_EXTRACT() and JSON_UNQUOTE().
        - Use CURDATE(), NOW(), or CURRENT_TIMESTAMP() for current date and time.
        """
    elif db == "mssql":
        return common_rules + """
        - Identifiers are case-insensitive. Always enclose them in square brackets ([]).
        - Schema usage: SQL Server uses schemas, with "dbo" as the default schema.
          - If tables are in the dbo schema, you can omit the schema name.
          - For tables in other schemas, always specify the schema: [schema_name].[table_name]
        - Example: SELECT [table_alias].[column_name] AS [column_alias] FROM [schema_name].[table_name] AS [table_alias]
        - Use CROSS APPLY and OUTER APPLY for more flexible join operations.
        - For JSON operations, use JSON_VALUE() and JSON_QUERY().
        - Use GETDATE() or CURRENT_TIMESTAMP for current date and time.
        """
    elif db == "sqlite":
        return common_rules + """
        - Identifiers are case-sensitive. Always enclose them in double quotes (") or square brackets ([]).
        - Schema usage: SQLite doesn't use schemas in the same way as other databases.
          - Each database is a single file and doesn't have separate schemas.
          - Do not use schema names before table names.
        - Example: SELECT "table_alias"."column_name" AS "column_alias" FROM "table_name" AS "table_alias"
        - Use SQLite-specific pragmas for performance tuning (e.g., PRAGMA cache_size, PRAGMA journal_mode).
        - Use the SQLite-specific json_extract() function for JSON operations.
        - Use date('now') or datetime('now') for current date and time.
        """
    else:
        return "Database type not recognized. Please specify 'postgres', 'mysql', 'mssql', or 'sqlite'."


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
       - Any assumptions you made or caveats.
       - Any potential ambiguities in the query and how you resolved them
    2. A list of the specific tables, columns, and definitions you referenced from the provided context.
    3. The SQL query to answer the user's question.

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
        {get_sql_rules(db)}
        - Ensure to apply appropriate filters based on the column type, and if necessary, cast the column to the correct data type for accurate querying.
        - Prefer to use appropriate date functions to filter date columns instead of static date filters. If necessary, cast the column to date to ensure accurate filtering.
        - If you encounter any JSON format column, use appropriate functions({get_json_extract_functions(db)}) for JSON extraction based on the database.
        - Current Date: {datetime.datetime.now().strftime('%Y-%m-%d')}
        - Available Date SQL Functions(Only use date functions from the provided list):
            {get_date_functions(db)}
        -  When addressing questions that suggest the utilization of a date column with a specific time granularity, adhere to the following steps:
            - Utilize the appropriate function({get_date_format_functions(db)}) to format the date column according to the granularity specified:
            • For years, display them in the YYYY format.
            • For quarters, represent them as Q1 YYYY, Q2 YYYY, Q3 YYYY, or Q4 YYYY.
            • For months, use the abbreviated month names with year (e.g., Jan - YYYY, Feb - YYYY, ..., Dec - YYYY).
            • For weeks, truncate the dates to a weekly basis.

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
    </response>

    Follow the XML format strictly. Answer with only XML response as it will be parsed as xml, no other extra words or formatting.
    """

    return prompt
