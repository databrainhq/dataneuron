import os
import yaml
from .db_operations.exceptions import ConfigurationError

CONFIG_PATH = 'database.yaml'


def load_context():
    context = {
        'tables': {},
        'relationships': {},
        'global_definitions': {},
        'database': {}
    }

    # Load table-specific context
    tables_path = os.path.join('context', 'tables')
    for filename in os.listdir(tables_path):
        if filename.endswith('.yaml'):
            with open(os.path.join(tables_path, filename), 'r') as f:
                table_data = yaml.safe_load(f)
                # Default to 'main' if schema is not specified
                schema_name = table_data.get('schema', 'main')
                table_name = table_data['table_name']
                full_name = f"{schema_name}.{table_name}"
                context['tables'][full_name] = table_data

    # Load relationships
    relationships_path = os.path.join('context', 'relationships.yaml')
    if os.path.exists(relationships_path):
        with open(relationships_path, 'r') as f:
            context['relationships'] = yaml.safe_load(f)

    # Load global definitions
    definitions_path = os.path.join('context', 'definitions.yaml')
    if os.path.exists(definitions_path):
        with open(definitions_path, 'r') as f:
            context['global_definitions'] = yaml.safe_load(f)

    # Load database configuration
    if not os.path.exists(CONFIG_PATH):
        raise ConfigurationError(
            f"Configuration file '{CONFIG_PATH}' not found. Please run the db-init command first.")

    try:
        with open(CONFIG_PATH, 'r') as file:
            config = yaml.safe_load(file)
        db_config = config.get('database', {})
        if not db_config:
            raise ConfigurationError(
                "No database configuration found in the YAML file.")
        # IMPORTANT not to send db_config itself
        context['database'] = db_config.get('name')
    except yaml.YAMLError as e:
        raise ConfigurationError(f"Error parsing YAML configuration: {str(e)}")
    except Exception as e:
        raise ConfigurationError(f"Error loading configuration: {str(e)}")

    return context
