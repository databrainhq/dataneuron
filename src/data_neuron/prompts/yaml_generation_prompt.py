def table_yaml_prompt(table_info):
    return f"""Generate a YAML file for the following table:

{table_info}

Follow this structure:
table_name: <name>
description: <brief description>
alias: <short alias>
columns:
  - name: <column name>
    description: <brief description>
    alias: <short alias>
    type: <data type>
    primary_key: <true/false>
    nullable: <true/false>

Ensure all fields are filled and accurate. Provide only the YAML content without additional explanations."""


def definitions_relationships_prompt(tables):
    return f"""Create two YAML files for the following tables: {', '.join(tables)}

1. definitions.yml:
   Include:
   - global_aliases:
     - table_aliases: for tables of any
     - common_terms: domain-specific terms or some some sample.
        - term: "term"
        - definition: "..."
   
2. relationships.yaml:
   For each pair of related tables, specify:
   - tables involved
   - relationship type (e.g., one-to-many)
   - brief description
   - foreign key details
   - reference key details

Provide only the YAML content for both files, separated by '---' on a new line."""
