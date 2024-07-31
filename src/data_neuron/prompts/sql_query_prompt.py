import datetime
import json
from ..utils.file_utils import format_yaml_for_prompt
from ..utils.date_functions import date_functions


def get_date_format_functions(database):
    if database == "mysql":
        return "DATE_FORMAT, QUARTER(), YEAR()... etc"
    elif database == "clickhouse":
        return "formatDateTime, toQuarter, toYear... etc"
    else:
        return "TO_CHAR"


def get_json_extract_functions(database):
    if database == "postgres":
        return "json_extract_path_text, jsonb_each_text"
    elif database == "mysql":
        return "JSON_EXTRACT, JSON_UNQUOTE"
    elif database == "mssql":
        return "JSON_VALUE, JSON_QUERY"
    elif database == "clickhouse":
        return "JSONExtractString, JSONExtract"
    else:
        return "json extract function"


def get_date_functions(db_name):
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
    elif db == "csv":
        return common_rules + """
        - Assume all columns are initially stored as VARCHAR due to CSV import.
        - Always cast columns to appropriate types before performing calculations or comparisons:
          - For numeric operations: CAST(column_name AS DECIMAL) or CAST(column_name AS INTEGER)
          - For date operations: CAST(column_name AS DATE) or TRY_CAST(column_name AS DATE)
        - Use appropriate aggregation functions: SUM(), AVG(), COUNT(), etc., with type casting.
        - For string operations, use functions like LOWER(), UPPER(), etc.
        - For JSON operations, use json_extract() or json_extract_string()
        - Available date functions: strftime(), date_part(), date_trunc(), etc.
        - IMPORTANT: When using window functions (like ROW_NUMBER(), RANK(), etc.) 
            - Window functions cannot be used directly in WHERE, HAVING, or GROUP BY clauses.
            - To filter or order by the result of a window function, you must use a subquery or CTE.
            - Always structure your query as follows when using window functions with filters:
                WITH ranked_data AS (
                    SELECT
                        ...,
                        RANK() OVER (...) AS rank
                    FROM
                        ...
                )
                SELECT *
                FROM ranked_data
                WHERE rank <= N
            - This ensures that the window function is calculated before any filtering is applied.
        - When working with dates:
            1. Always cast date strings to DATE type before using in date functions:
               - Use: CAST(date_column AS DATE) or TRY_CAST(date_column AS DATE)
            2. For date differences, use the following structure:
               DATEDIFF('unit', CAST(start_date AS DATE), CAST(end_date AS DATE))
               Where 'unit' can be 'day', 'month', 'year', etc.
            3. When using date_trunc or other date functions, always cast to DATE first:
               DATE_TRUNC('month', CAST(date_column AS DATE))
            4. For parsing date strings, use:
               STRPTIME(date_string, 'format')
               Example: STRPTIME(date_column, '%Y-%m-%d')
            5. When comparing dates, ensure both sides are cast to DATE:
               WHERE CAST(date_column AS DATE) > CAST('2023-01-01' AS DATE)
            6. For date arithmetic, use INTERVAL:
               CAST(date_column AS DATE) + INTERVAL '1 day'
            7. Always assume date columns from CSV files are stored as VARCHAR and need explicit casting.
            8. When calculating average time differences, cast the result to ensure decimal precision:
               AVG(CAST(DATEDIFF('day', CAST(start_date AS DATE), CAST(end_date AS DATE)) AS DECIMAL(10,2)))
            9. For formatting dates, use STRFTIME():
               STRFTIME(CAST(date_column AS DATE), '%b - %Y') for 'Mon - YYYY' format
            10. When ordering by formatted date strings, use DATE_TRUNC() in the ORDER BY clause:
                ORDER BY DATE_TRUNC('month', CAST(date_column AS DATE))
            11. If you need to group by month and year, use DATE_TRUNC() in the GROUP BY clause as well:
                GROUP BY DATE_TRUNC('month', CAST(date_column AS DATE))
            12. When selecting formatted dates, you can combine DATE_TRUNC() and STRFTIME():
                STRFTIME(DATE_TRUNC('month', CAST(date_column AS DATE)), '%b - %Y') AS formatted_date
        
        Remember, proper date handling and casting is crucial for correct query execution in DuckDB, especially when working with CSV data.
    """

    elif db == "clickhouse":
        return common_rules + """
        - Identifiers are case-sensitive. Always enclose them in backticks (`).
        - Schema usage: ClickHouse uses databases, not schemas.
          - Specify the database name before the table name: `database_name`.`table_name`
        - Example: SELECT `table_alias`.`column_name` AS `column_alias` FROM `database_name`.`table_name` AS `table_alias`
        - Use FINAL keyword after table name to get the latest version of data in tables with ReplacingMergeTree engine.
        - For JSON operations, use JSONExtractString() or JSONExtract().
        - Use toDate(), toDateTime() for date and time conversions.
        - Utilize ClickHouse-specific functions like arrayJoin() for working with array columns.
        - For full-text search, consider using the ClickHouse-specific trigram index.
        - Use materialized views for precomputing complex aggregations.
        - Take advantage of ClickHouse's columnar storage by minimizing the number of columns in your queries.
        - Use PREWHERE for more efficient filtering on large tables.
        - Prefer using Int64 for numeric types when possible, as it's more efficient in ClickHouse.
        - Use LowCardinality data type for columns with a small number of frequently occurring string values.
        """
    else:
        return "Database type not recognized. Please specify 'postgres', 'mysql', 'mssql', or 'sqlite' or 'csv'"


def sql_query_prompt(query, context, db):
    context_prompt = "Database Context:\n\n"

    given_db = 'duckdb' if db == 'csv' else db
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

    IMPORTANT: Only use tables and columns that are explicitly defined in the provided context.
    Do not assume the existence of any tables or columns that are not listed.

    SQL query guidelines:
    - Only generate SELECT statements, non-write, non-destructive queries.
    - Only reference tables, coilumns given in the context.
    - Always use fully qualified table names (schema.table_name) in your SQL queries.
    - Use the provided aliases and global definitions to interpret the user's query accurately.
    - Use actual columns and tables to query. Never use alias to query the column.
        example:
            context: users.id with alias uid
            correct: SELECT id as uid ..
            incorrect: SELECT uid
    - Based on the query, formulate an SQL query targeting the database: "{given_db}".
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
            Tables: schema1.table1, schema2.table2
            Columns: schema1.table1.column1, schema2.table2.column2
            Definitions: definition1, definition2
        </references>
    </response>

    Follow the XML format strictly. Answer with only XML response as it will be parsed as xml, no other extra words or formatting.
    """

    return prompt
