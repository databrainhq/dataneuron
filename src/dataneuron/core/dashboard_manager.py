import os
import re
import yaml
from ..db_operations.factory import DatabaseFactory
from ..prompts.report_generation_prompt import report_generation_prompt
from ..api.main import call_neuron_vision_api, call_neuron_api
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

    def generate_report_html(self, dashboard_name, instruction, image_path=None):
        dashboard = self.load_dashboard(dashboard_name)
        if not dashboard:
            return f"<html><body><h1>Error: Dashboard '{dashboard_name}' not found.</h1></body></html>"

        results = self.execute_dashboard_queries(dashboard_name)
        results_yaml = yaml.dump(results, default_flow_style=False)

        prompt = report_generation_prompt(
            dashboard_name, results_yaml, instruction)

        if image_path and os.path.exists(image_path):
            response = call_neuron_vision_api(prompt, image_path)
        else:
            response = call_neuron_api(prompt)

        return self.extract_html_from_response(response)

    @staticmethod
    def extract_html_from_response(response):
        html_pattern = re.compile(
            r'<!DOCTYPE html>.*?</html>', re.DOTALL | re.IGNORECASE)
        match = html_pattern.search(response)

        if match:
            html_content = match.group(0)
        else:
            html_content = response

        return html_content
