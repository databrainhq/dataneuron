from flask import Flask, request, jsonify, Response
from .core.data_neuron import DataNeuron
from .core.dashboard_manager import DashboardManager
from .core.context_loader import ContextLoader
from .utils.serialization import ensure_serializable, convert_to_serializable
import traceback


def create_app(config=None):
    app = Flask(__name__)

    if config:
        app.config.from_object(config)

    def get_dataneuron(context=None):
        dataneuron = DataNeuron(db_config='database.yaml', context=context)
        dataneuron.initialize()
        return dataneuron

    def get_dashboard_manager():
        dashboard_manager = DashboardManager()
        return dashboard_manager

    @app.route('/chat', methods=['POST'])
    def chat():
        data = request.json
        messages = data.get('messages', [])
        context_name = data.get('context_name')
        client_id = data.get('client_id')

        if not messages or not isinstance(messages, list):
            return jsonify({"error": "messages must be a non-empty list"}), 400

        try:
            dn = get_dataneuron(context_name)

            # Set chat history if there are previous messages
            if len(messages) > 1:
                dn.set_chat_history(messages[:-1])

            # Set client context if client_id is provided
            if client_id:
                # New line to set client context
                dn.set_client_context(client_id)

            # Get the last user message
            last_user_message = next((msg['content'] for msg in reversed(
                messages) if msg['role'] == 'user'), None)
            if last_user_message is None:
                return jsonify({"error": "No user message found"}), 400

            sql, response = dn.chat(last_user_message)
            serializable_response = ensure_serializable(response)
            return jsonify({"response": serializable_response, "sql": sql})

        except Exception as e:
            app.logger.error(
                f"Error in chat endpoint: {str(e)}", exc_info=True)
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
            dm = get_dashboard_manager()
            html_content = dm.generate_report_html(
                dashboard_name, instruction, image_path)
            return Response(html_content, mimetype='text/html')
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.route('/dashboards', methods=['GET'])
    def get_dashboards():
        try:
            dm = get_dashboard_manager()
            dashboards = dm.list_dashboards()
            return jsonify({"dashboards": dashboards})
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.route('/dashboards/<dashboard_id>', methods=['GET'])
    def get_dashboard(dashboard_id):
        try:
            dm = get_dashboard_manager()
            dashboard = dm.load_dashboard(dashboard_id)
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

            dm = get_dashboard_manager()
            dashboard = dm.load_dashboard(dashboard_id)
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

            dn = get_dataneuron()
            result = dn.db.execute_query_with_column_names(sql_query)

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

    @app.route('/execute_query', methods=['POST'])
    def execute_query():
        data = request.json
        sql_query = data.get('sql_query')
        context_name = data.get('context_name')
        client_id = data.get('client_id')

        if not sql_query:
            return jsonify({"error": "sql_query is required"}), 400

        try:
            dn = get_dataneuron(context_name)
            if client_id:
                dn.set_client_context(client_id)
            result = dn.execute_query_with_column_names(sql_query)
            return jsonify(result)
        except Exception as e:
            return jsonify({"error": str(e)}), 500
    return app


def run_server(host='0.0.0.0', port=8084, debug=False):
    app = create_app()
    app.run(host=host, port=port, debug=debug)


if __name__ == '__main__':
    run_server()
