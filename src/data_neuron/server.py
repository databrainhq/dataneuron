from flask import Flask, request, jsonify, Response
from .db_init_cmd.main import init_database_config
from .context_init_cmd.main import init_context
from .ask_cmd.main import query as ask_query
from .chat_cmd.main import process_chat_message
from .db_operations.factory import DatabaseFactory
from .report_cmd.main import generate_report_html, list_dashboards, load_dashboard
import json
import traceback
import os

app = Flask(__name__)


@app.route('/chat', methods=['POST'])
def chat():
    data = request.json
    messages = data.get('messages', [])
    context_name = data.get('context_name')

    if not messages or not isinstance(messages, list):
        return jsonify({"error": "messages must be a non-empty list"}), 400

    # Find the index of the last user message
    last_user_index = next((i for i in range(len(messages) - 1, -1, -1)
                            if messages[i]['role'] == 'user'), None)

    if last_user_index is None:
        return jsonify({"error": "No user message found"}), 400

    # Extract the last user message and remove it from the list
    user_message = messages.pop(last_user_index)['content']

    response = process_chat_message(user_message, context_name, messages)
    serializable_response = ensure_serializable(response)

    return jsonify({"response": serializable_response})


@app.route('/reports', methods=['POST'])
def generate_report():
    data = request.json
    dashboard_name = data.get('dashboard_name')
    instruction = data.get('instruction')
    image_path = data.get('image_path')

    if not dashboard_name:
        return jsonify({"error": "dashboard_name is required"}), 400
    if not instruction:
        return jsonify({"error": "instruction is required"}), 400

    try:
        html_content = generate_report_html(
            dashboard_name, instruction, image_path)
        return Response(html_content, mimetype='text/html')
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/dashboards', methods=['GET'])
def get_dashboards():
    try:
        dashboards = list_dashboards()
        return jsonify({"dashboards": dashboards})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/dashboards/<dashboard_id>', methods=['GET'])
def get_dashboard(dashboard_id):
    try:
        dashboard = load_dashboard(dashboard_id)
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

        dashboard = load_dashboard(dashboard_id)
        if not dashboard:
            return jsonify({"error": "Dashboard not found"}), 404

        metric = next(
            (m for m in dashboard['metrics'] if m['name'] == metric_name), None)
        if not metric:
            return jsonify({"error": "Metric not found"}), 404

        sql_query = metric['sql_query']

        # Apply parameters to the SQL query
        for key, value in parameters.items():
            placeholder = f":{key}"
            sql_query = sql_query.replace(placeholder, str(value))

        db = DatabaseFactory.get_database()
        result = db.execute_query_with_column_names(sql_query)

        if isinstance(result, tuple) and len(result) == 2:
            data, columns = result
            # Convert Row objects to dictionaries
            serializable_data = convert_to_serializable(data, columns)
            return jsonify({
                "columns": columns,
                "data": serializable_data
            })
        else:
            return jsonify({"error": "Unexpected result format"}), 500

    except Exception as e:
        print(traceback.format_exc())  # This will print the full stack trace
        return jsonify({"error": str(e)}), 500


def run_server(host='0.0.0.0', port=8084, debug=False):
    app.run(host=host, port=port, debug=debug)


if __name__ == '__main__':
    run_server()


def convert_to_serializable(data, columns):
    return [dict(zip(columns, row)) for row in data]


def ensure_serializable(obj):
    try:
        json.dumps(obj)
        return obj
    except (TypeError, OverflowError):
        if isinstance(obj, dict):
            return {k: ensure_serializable(v) for k, v in obj.items()}
        elif isinstance(obj, (list, tuple)):
            return [ensure_serializable(item) for item in obj]
        elif hasattr(obj, '__dict__'):
            return ensure_serializable(obj.__dict__)
        else:
            return str(obj)
