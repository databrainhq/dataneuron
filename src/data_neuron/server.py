from flask import Flask, request, jsonify, Response
from .db_init_cmd.main import init_database_config
from .context_init_cmd.main import init_context
from .ask_cmd.main import query as ask_query
from .chat_cmd.main import process_chat_message
from .report_cmd.main import generate_report_html
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
    return jsonify({"response": response})


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


def run_server(host='0.0.0.0', port=8084, debug=False):
    app.run(host=host, port=port, debug=debug)


if __name__ == '__main__':
    run_server()
