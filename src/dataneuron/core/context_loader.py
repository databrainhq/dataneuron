import os
import yaml
from ..utils.file_utils import format_yaml_for_prompt


class ContextLoader:
    def __init__(self, context_name: str, config_path: str = 'database.yaml'):
        self.context_name = context_name
        self.config_path = config_path
        self.context_dir = os.path.join('context', context_name)
        self.context = {
            'tables': {},
            'relationships': {},
            'global_definitions': {},
            'database': {},
            'client_info': {}
        }

    def load(self):
        """Load the entire context."""
        self._load_tables()
        self._load_relationships()
        self._load_global_definitions()
        self._load_sample_data()
        self._load_client_tables()
        return self.context

    def get_formatted_context(self) -> str:
        context_prompt = "Database Context:\n\n"

        # Format table information
        context_prompt += "Tables:\n"
        for table_name, table_data in self.context["tables"].items():
            context_prompt += f"  {table_name}:\n"
            context_prompt += format_yaml_for_prompt(
                table_data).replace("\n", "\n    ")
            context_prompt += "\n"

        # Format relationships
        context_prompt += "\nRelationships:\n"
        context_prompt += format_yaml_for_prompt(
            self.context["relationships"]).replace("\n", "\n  ")

        # Format global definitions
        context_prompt += "\nGlobal Definitions:\n"
        context_prompt += format_yaml_for_prompt(
            self.context["global_definitions"]).replace("\n", "\n  ")

        return context_prompt

    def _load_tables(self):
        """Load table-specific context."""
        tables_path = os.path.join(self.context_dir, 'tables')
        for filename in os.listdir(tables_path):
            if filename.endswith('.yaml'):
                self._load_table(os.path.join(tables_path, filename))

    def _load_table(self, file_path: str):
        """Load a single table's context."""
        if os.path.getsize(file_path) > 0:  # Check if file is not empty
            with open(file_path, 'r') as f:
                table_data = yaml.safe_load(f)
                if table_data and isinstance(table_data, dict):
                    full_name = table_data.get('full_name')
                    if full_name:
                        self.context['tables'][full_name] = table_data
                    else:
                        print(
                            f"Warning: 'full_name' not found in {os.path.basename(file_path)}. Skipping this table.")
                else:
                    print(
                        f"Warning: Invalid or empty YAML content in {os.path.basename(file_path)}. Skipping this table.")

    def _load_relationships(self):
        """Load relationships context."""
        relationships_path = os.path.join(
            self.context_dir, 'relationships.yaml')
        if os.path.exists(relationships_path):
            with open(relationships_path, 'r') as f:
                self.context['relationships'] = yaml.safe_load(f)

    def _load_global_definitions(self):
        """Load global definitions context."""
        definitions_path = os.path.join(self.context_dir, 'definitions.yaml')
        if os.path.exists(definitions_path):
            with open(definitions_path, 'r') as f:
                self.context['global_definitions'] = yaml.safe_load(f)

    def _load_sample_data(self):
        """Load sample data from YAML file."""
        sample_data_path = os.path.join(self.context_dir, 'sample_data.yaml')
        if os.path.exists(sample_data_path):
            with open(sample_data_path, 'r') as f:
                self.context['sample_data'] = yaml.safe_load(f)

    def _load_client_tables(self):
        """Load client-specific table information."""
        client_info_path = os.path.join(self.context_dir, 'client_info.yaml')
        if os.path.exists(client_info_path):
            with open(client_info_path, 'r') as f:
                self.context['client_info'] = yaml.safe_load(f)
