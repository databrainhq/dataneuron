# src/dataneuron/core/context_initializer.py

import os
import click
import yaml
from ..core.data_neuron import DataNeuron
from ..core.dashboard_manager import DashboardManager
from ..utils.print import print_header, print_info, print_success, print_warning, print_prompt
from ..db_operations.error_handler import handle_database_errors
from ..db_operations.database_helpers import DatabaseHelper


class ContextInitializer:
    def __init__(self, db_config='database.yaml'):
        self.dataneuron = DataNeuron(db_config=db_config, context=None)
        self.dataneuron.initialize()
        self.db = self.dataneuron.db
        self.dashboard_manager = DashboardManager()
        self.db_helper = DatabaseHelper(self.db.db_type, self.db)

    @handle_database_errors
    def init_context(self):
        print_header(
            "Setting up a new context (semantic layer) for your database...")
        context_name = click.prompt(
            "Please enter a name for this context (e.g., customer_success or product_analytics)", type=str)
        context_dir = os.path.join('context', context_name)

        if os.path.exists(context_dir):
            overwrite = click.confirm(
                f"Context '{context_name}' already exists. Do you want to overwrite it?", default=False)
            if not overwrite:
                print_info("Context initialization cancelled.")
                return

        print_warning(
            f"This will create a folder {context_dir} in the current directory. Existing files will be overwritten.")
        print_prompt("You can edit it anytime...")
        print_warning("Please choose a set of 10 or fewer tables..\n")

        print_info("🗄️ Fetching tables from the database")
        all_tables = self.db.get_table_list()
        chosen_tables = self._choose_tables(all_tables)

        os.makedirs(os.path.join(context_dir, 'tables'), exist_ok=True)

        sample_data = {}
        for table in chosen_tables:
            yaml_content = self._generate_yaml_for_table(
                table['schema'], table['table'])
            with open(os.path.join(context_dir, 'tables', f'{table["table"]}.yaml'), 'w') as f:
                f.write(yaml_content)
            print_success(f"Generated YAML for table: {table['table']}")
            table_name = f"{table['schema']}.{table['table']}"
            res = self._generate_sample_data(table_name)
            sample_data[table_name] = res

        with open(os.path.join(context_dir, 'sample_data.yaml'), 'w') as f:
            yaml.dump(sample_data, f)
        print_success("Generated sample_data.yaml")

        print_info("Generating definitions and relationships...")
        try:
            definitions_yaml, relationships_yaml = self._generate_definitions_and_relationships(
                chosen_tables)

            with open(os.path.join(context_dir, 'definitions.yaml'), 'w') as f:
                f.write(definitions_yaml)
            print_success("Generated definitions.yaml")

            with open(os.path.join(context_dir, 'relationships.yaml'), 'w') as f:
                f.write(relationships_yaml)
            print_success("Generated relationships.yaml")

            print_success(
                f"Initialization complete for context: {context_name}")

            # Initialize ContextLoader with the new context
            self.dataneuron.set_context(context_name)

        except Exception as e:
            print_warning(f"An error occurred: {str(e)}")

    def _choose_tables(self, all_tables):
        chosen_tables = []
        for i in range(0, len(all_tables), 10):
            batch = all_tables[i:i+10]
            click.echo(
                "Choose tables (enter numbers separated by commas, or 'all' for all, 'skip' for next batch, 'done' to finish):")
            for idx, table in enumerate(batch, start=1):
                table_name = f"{table['schema']}.{table['table']}"
                click.echo(f"{idx}. {table_name}")
            choice = click.prompt("Your choice").lower()
            if choice == 'all':
                chosen_tables.extend(batch)
                break
            elif choice == 'skip':
                continue
            elif choice == 'done':
                break
            else:
                chosen_tables.extend([batch[int(i)-1] for i in choice.split(',')
                                      if i.strip().isdigit() and 0 < int(i) <= len(batch)])
        return chosen_tables

    def _generate_yaml_for_table(self, schema, table):
        table_info = self.db.get_table_info(schema, table)
        from ..prompts.yaml_generation_prompt import table_yaml_prompt
        from ..api.main import stream_neuron_api

        prompt = table_yaml_prompt(table_info, self.db.db_type)
        system_prompt = "You are a helpful assistant that generates YAML content based on database table information."

        yaml_content = ""
        for chunk in stream_neuron_api(prompt, instruction_prompt=system_prompt):
            click.echo(chunk, nl=False)
            yaml_content += chunk
        return yaml_content

    def _generate_definitions_and_relationships(self, tables):
        from ..prompts.yaml_generation_prompt import definitions_relationships_prompt
        from ..api.main import stream_neuron_api

        table_names = [table['table'] for table in tables]
        prompt = definitions_relationships_prompt(table_names, self.db.db_type)
        system_prompt = "You are a helpful assistant that generates YAML content for database definitions and relationships."

        yaml_content = ""
        for chunk in stream_neuron_api(prompt, instruction_prompt=system_prompt):
            yaml_content += chunk

        # Split the YAML content into definitions and relationships
        definitions_yaml, relationships_yaml = yaml_content.split('---')
        return definitions_yaml.strip(), relationships_yaml.strip()

    def _generate_sample_data(self, table):
        # Assuming schema is not needed here
        query = self.db_helper.top_few_records(
            column_name="*", table_name=table, limit=5)

        result = self.db.execute_query_with_column_names(query)
        if isinstance(result, tuple) and len(result) == 2:
            data, columns = result
            return [dict(zip(columns, row)) for row in data]
        else:
            print_warning(
                f"Unexpected result format when generating sample data for table {table}")
            print(f"Result: {result}")
            return {'columns': [], 'data': []}
