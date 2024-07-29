def table_yaml_prompt(table_info, db_type):

    return f"""Generate a YAML file for the following table, targeting a {db_type} database:
{table_info}
Follow this structure:
table_name: <name>
schema_name: <name>  
full_name: <schema_name>.<table_name>
description: <brief description>
alias: <short alias>
columns:
  - name: <column name>
    description: <brief description>
    alias: <short alias>
    type: <data type specific to {db_type}>
    primary_key: <true/false>
    nullable: <true/false>
Fill the section whatever you can fill. You can assume and fill the ones that you're not sure.
Important guidelines:
- Use appropriate data types and conventions for {db_type}
- For MySQL and SQLite, omit the schema_name field entirely
- For MSSQL, PostgreSQL, and DuckDB, include the schema_name as appropriate
- Ensure column names do not include any schema or table prefixes
- Always include the full_name field as <schema_name>.<table_name>, using 'main' for SQLite and DuckDB if no schema is specified
Provide only the YAML content without additional explanations or markdown formatting."""


def definitions_relationships_prompt(tables, db_type):
    schema_instruction = {
        "mysql": "# MySQL doesn't use schemas in the same way, use only table names",
        "mssql": "# For MSSQL, use 'schema_name.table_name' format if schemas are used",
        "postgres": "# For PostgreSQL, use 'schema_name.table_name' format",
        "sqlite": "# SQLite doesn't use schemas, use 'main.table_name' format",
        "duckdb": "# For DuckDB, use 'schema_name.table_name' format, with 'main' as the default schema"
    }.get(db_type, "# Use appropriate schema notation for your database if applicable")

    # Handle both list of strings and list of dictionaries
    if tables and isinstance(tables[0], dict):
        table_list = ', '.join([f"{t['schema']}.{t['table']}" for t in tables])
    elif tables and isinstance(tables[0], str):
        table_list = ', '.join(tables)
    else:
        raise ValueError("Unexpected table format")

    return f"""Create two YAML files for the following tables in a {db_type} database: {table_list}
1. definitions.yml:
   Include:
   - global_aliases:
     - table_aliases: for tables of any
     - common_terms: domain-specific terms or some sample.
        - term: "term"
        - definition: "..."
  
   Definitions yml should only have the keys as given in the examples, nothing extra
   
2. relationships.yaml:
   For each pair of related tables, specify:
   - tables involved
   - relationship type (e.g., one-to-many)
   - brief description
   - foreign key details
   - reference key details

Fill the section whatever you can fill. You can assume and fill the ones that you're not sure.
Important guidelines:
- {schema_instruction}
- Use data types and conventions appropriate for {db_type}
- Ensure all references to tables and columns are consistent across both files
- For MySQL, use only table names without schema prefixes
- For SQLite and DuckDB, use 'main' as the default schema if not specified
- For MSSQL and PostgreSQL, include schema names where appropriate
- Always use the format schema_name.table_name when referring to tables, even for databases that don't typically use schemas
Provide only the YAML content (no markdown formatting) for both files, separated by '---' on a new line."""
