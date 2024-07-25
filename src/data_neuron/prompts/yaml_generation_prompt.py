def table_yaml_prompt(table_info):
    return f"""Generate a YAML file for the following table:

{table_info}

Follow this structure:
table_name: <name>
schema_name: <name>
description: <brief description>
alias: <short alias>
columns:
  - name: <column name>
    description: <brief description>
    alias: <short alias>
    type: <data type>
    primary_key: <true/false>
    nullable: <true/false>

Fill the section whatever you can fill. You can assume and fill the ones that you're not sure.
Provide only the YAML content without additional explanations or things like ```yaml just the content."""


def definitions_relationships_prompt(tables):
    return f"""Create two YAML files for the following tables: {', '.join(tables)}

1. definitions.yml:
   Include:
   - global_aliases:
     - table_aliases: for tables of any
     - common_terms: domain-specific terms or some some sample.
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
Provide only the YAML content (no other things like ```yaml) for both files, separated by '---' on a new line."""
