def table_yaml_prompt(table_info, db_type):
    schema_instruction = {
        "mysql": "# MySQL doesn't use schemas in the same way, leave this blank",
        "mssql": "# For MSSQL, typically use 'dbo' unless specified otherwise",
        "postgres": "# For PostgreSQL, typically use 'public' unless specified otherwise",
        "sqlite": "# SQLite doesn't use schemas, leave this blank"
    }.get(db_type, "# Specify the schema if applicable for your database")

    return f"""Generate a YAML file for the following table, targeting a {db_type} database:
{table_info}
Follow this structure:
table_name: <name>
schema_name: <name>  {schema_instruction}
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
- For MSSQL and PostgreSQL, include the schema_name as appropriate
- Ensure column names do not include any schema or table prefixes
Provide only the YAML content without additional explanations or markdown formatting."""


def definitions_relationships_prompt(tables, db_type):
    schema_instruction = {
        "mysql": "# MySQL doesn't use schemas in the same way, use only table names",
        "mssql": "# For MSSQL, use 'schema_name.table_name' format if schemas are used",
        "postgres": "# For PostgreSQL, use 'schema_name.table_name' format",
        "sqlite": "# SQLite doesn't use schemas, use only table names"
    }.get(db_type, "# Use appropriate schema notation for your database if applicable")

    return f"""Create two YAML files for the following tables in a {db_type} database: {', '.join(tables)}
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
- For MySQL and SQLite, use only table names without schema prefixes
- For MSSQL and PostgreSQL, include schema names where appropriate
Provide only the YAML content (no markdown formatting) for both files, separated by '---' on a new line."""
