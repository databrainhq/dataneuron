import os
import yaml
from ..db_operations.factory import DatabaseFactory
from ..utils.print import print_warning


class DashboardManager:
    def __init__(self, dashboards_dir="dashboards"):
        self.dashboards_dir = dashboards_dir
        os.makedirs(self.dashboards_dir, exist_ok=True)

    def list_dashboards(self):
        return [f.split('.')[0] for f in os.listdir(self.dashboards_dir) if f.endswith('.yml')]

    def load_dashboard(self, dashboard_name):
        dashboard_file = os.path.join(
            self.dashboards_dir, f"{dashboard_name}.yml")
        if not os.path.exists(dashboard_file):
            return None
        with open(dashboard_file, 'r') as f:
            return yaml.safe_load(f)

    def save_to_dashboard(self, dashboard_name, metric_name, query):
        dashboard_file = os.path.join(
            self.dashboards_dir, f"{dashboard_name}.yml")
        if os.path.exists(dashboard_file):
            with open(dashboard_file, 'r') as f:
                dashboard = yaml.safe_load(f)
        else:
            dashboard = {'metrics': []}

        new_metric = {
            'name': metric_name,
            'sql_query': query
        }
        dashboard['metrics'].append(new_metric)

        with open(dashboard_file, 'w') as f:
            yaml.dump(dashboard, f)

    def view_dashboard(self, dashboard_name):
        dashboard = self.load_dashboard(dashboard_name)
        if not dashboard:
            return None
        return dashboard['metrics']

    def execute_dashboard_queries(self, dashboard_name):
        dashboard = self.load_dashboard(dashboard_name)
        if not dashboard:
            return None

        db = DatabaseFactory.get_database()
        results = {}
        for metric in dashboard['metrics']:
            sql_query = metric['sql_query']
            try:
                result = db.execute_query(sql_query)
                results[metric['name']] = result
            except Exception as e:
                print_warning(
                    f"Error executing query for metric '{metric['name']}': {str(e)}")
                results[metric['name']] = f"Error: {str(e)}"
        return results
