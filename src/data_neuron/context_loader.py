import os
import yaml


def load_context():
    context = {
        'tables': {},
        'relationships': {},
        'global_definitions': {}
    }

    # Load table-specific context
    tables_path = os.path.join('context', 'tables')
    for filename in os.listdir(tables_path):
        if filename.endswith('.yaml'):
            with open(os.path.join(tables_path, filename), 'r') as f:
                table_data = yaml.safe_load(f)
                context['tables'][table_data['table_name']] = table_data

    # Load relationships
    with open(os.path.join('context', 'relationships.yaml'), 'r') as f:
        context['relationships'] = yaml.safe_load(f)

    # Load global definitions
    with open(os.path.join('context', 'definitions.yaml'), 'r') as f:
        context['global_definitions'] = yaml.safe_load(f)

    return context
