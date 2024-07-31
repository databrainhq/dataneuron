# src/data_neuron/server.py

from flask import Flask, request, jsonify, Response
from .core import DataNeuron
from .core.dashboard_manager import DashboardManager
from .core.context_loader import ContextLoader
from .utils.serialization import ensure_serializable, convert_to_serializable
import traceback

app = Flask(__name__)

data_neuron = DataNeuron(db_config='database.yaml', context=None)
data_neuron.initialize()
dashboard_manager = DashboardManager()


@app.route('/chat', methods=['POST'])
def chat():
    data = request.json
    messages = data.get('messages', [])
    context_name = data.get('context_name')

    if not messages or not isinstance(messages, list):
        return jsonify({"error": "messages must be a non-empty list"}), 400

    last_user_index = next((i for i in range(len(messages) - 1, -1, -1)
                            if messages[i]['role'] == 'user'), None)

    if last_user_index is None:
        return jsonify({"error": "No user message found"}), 400

    user_message = messages.pop(last_user_index)['content']

    try:
        if context_name:
            context_loader = ContextLoader(context_name)
            data_neuron.context = context_loader.load()

        response = data_neuron.chat(user_message)
        serializable_response = ensure_serializable(response)
        return jsonify({"response": serializable_response})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/reports', methods=['POST'])
def generate_report():
    data = request.json
    dashboard_name = data.get('dashboard_name')
    instruction = data.get('instruction')
    image_path = data.get('image_path')

    if not dashboard_name or not instruction:
        return jsonify({"error": "dashboard_name and instruction are required"}), 400

    try:
        html_content = dashboard_manager.generate_report_html(
            dashboard_name, instruction, image_path)
        return Response(html_content, mimetype='text/html')
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/dashboards', methods=['GET'])
def get_dashboards():
    try:
        dashboards = dashboard_manager.list_dashboards()
        return jsonify({"dashboards": dashboards})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/dashboards/<dashboard_id>', methods=['GET'])
def get_dashboard(dashboard_id):
    try:
        dashboard = dashboard_manager.load_dashboard(dashboard_id)
        if dashboard:
            return jsonify(dashboard)
        else:
            return jsonify({"error": "Dashboard not found"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/execute-metric', methods=['POST'])
def execute_metric():
    try:
        data = request.json
        dashboard_id = data.get('dashboard_id')
        metric_name = data.get('metric_name')
        parameters = data.get('parameters', {})

        if not dashboard_id or not metric_name:
            return jsonify({"error": "dashboard_id and metric_name are required"}), 400

        dashboard = dashboard_manager.load_dashboard(dashboard_id)
        if not dashboard:
            return jsonify({"error": "Dashboard not found"}), 404

        metric = next(
            (m for m in dashboard['metrics'] if m['name'] == metric_name), None)
        if not metric:
            return jsonify({"error": "Metric not found"}), 404

        sql_query = metric['sql_query']

        for key, value in parameters.items():
            placeholder = f":{key}"
            sql_query = sql_query.replace(placeholder, str(value))

        result = data_neuron.db.execute_query_with_column_names(sql_query)

        if isinstance(result, tuple) and len(result) == 2:
            data, columns = result
            serializable_data = convert_to_serializable(data, columns)
            return jsonify({
                "columns": columns,
                "data": serializable_data
            })
        else:
            return jsonify({"error": "Unexpected result format"}), 500

    except Exception as e:
        print(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


def run_server(host='0.0.0.0', port=8084, debug=False):
    app.run(host=host, port=port, debug=debug)


if __name__ == '__main__':
    run_server()
