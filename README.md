# Data Neuron

Data Neuron is a simple framework for you to
chat with your data in natural language through python sdk, REST API or CLI directly and with an easy to maintain and continually improving semantic layer in the form yml files.

⭐ If you find DataNeuron useful, please consider giving us a star on GitHub! Your support helps us continue to innovate and deliver exciting features

### Quick Start

- Install the library using `pip`.
- Choose your specific set of tables and label them with alias, description, business specific glossary/definitions, client/tenant tables(if it is for your end users).
- Chat in cli using `dnn --chat <contextname>` to test and validate how LLM performs
- Once your semantic layer is ready, start integrating within your existing python app through our sdk like
  `from dataneuron import DataNeuron` and build internal slack app or build customer facing chatbot or email reports. Or deploy your semantic layer + dataneuron as an API endpoint to AWS lambda or VPS machine.

Currently supports SQLite, PostgreSQL, MySQL, MSSQL, CSV files(through duckdb), Clickhouse. Works with major LLMs like Claude (default), OpenAI, LLAMA etc(through groq, nvidia, ..), OLLAMA.

## Quick Usage

- Install `pip install dataneuron[mssql, pdf]`
- Set he LLM key in an enviornment variable in your system, by default it uses claude `CLAUDE_API_KEY`
- Initialize database config: `dnn --db-init <database_type>`
- Generate context: `dnn --init`
- Start chat mode: `dnn --chat <contextname>` and save it as metrics to dashboards locally as yaml files.
- Get html pdf reports for your dashboard: `dnn --report`
- Run the API server to access chat, reports, dashboards, metrics: `dnn --server` (See API section for more)
- Deploy the server through AWS lambda or traditional VPS machine.
- If you have an existing Django or Flask or python project you can use the DataNeuron class from the pacakge directly like shown below:

```python
from dataneuron import DataNeuron

# Initialize DataNeuron
dn = DataNeuron(db_config='database.yaml', context='your_context_name')
dn.initialize()

dn.set_client_context("userid") # optional: if you want to make it scoped specific to your customer/tenant
# Ask a question
question = "How many users do we have?"
result = dn.query(question)

print(f"SQL Query: {result['sql']}")
print(f"Result: {result['result']}")
```

#### Base setup

https://github.com/user-attachments/assets/2108cce7-c48c-4a45-b1c6-f7bde71c635c

#### Reports pdf

https://github.com/user-attachments/assets/de71a220-4bd9-4f53-b245-064fcaca85bb

### As API

https://github.com/user-attachments/assets/0fd477cd-ef8b-44ed-993a-b1ad16cfd82a

https://github.com/user-attachments/assets/8d363c0a-e12a-47ff-b4e4-e5f0bf302224

## Features

- Support for multiple database types (SQLite, PostgreSQL, MySQL, MSSQL, CSV files(through duckdb), Clickhouse)
- Natural language to SQL query conversion
- Interactive chat mode for continuous database querying
- Multiple context management, you can create and manage multiple contexts for your customer_succes team, product team etc
- Automatic context generation from database schema
- Customizable context for improved query accuracy
- Support for various LLM providers (Claude, OpenAI, Azure, Custom, Ollama)
- Optimized for smaller database subsets (up to 10-15 tables)
- API server that can be deployed to AWS lambda or traditional server that support python flask.
- Through API you can list the /dashboards, metrics and also query individual metric and also chat with your context and generate feature rich HTML report

## Installation

Data Neuron can be installed with different database support options:

1. Base package (SQLite support only):

   ```
   pip install dataneuron
   ```

2. With PostgreSQL support:

   ```
   pip install dataneuron[postgres]
   ```

3. With MySQL support:

   ```
   pip install dataneuron[mysql]
   ```

4. With MSSQL support:

   ```
   pip install dataneuron[mssql]
   ```

5. With all database supports:

   ```
   pip install dataneuron[all]
   ```

6. With CSV support:

   ```
   pip install dataneuron[csv]
   ```

7. With Clickhouse support:

   ```
   pip install dataneuron[clickhouse]
   ```

Note: if you use zsh, you might have to use quotes around the package name like. For csv right now it doesn't
support nested folder structure just a folder with csv files, each csv will be treated as a table.

```
pip install "dataneuron[mysql]"
```

If you wanted the report generation with pdf in your cli, you have to include `pdf` along with your db as extra dependencies.

```
  pip install "dataneuron[mssql,pdf]"
```

## Quick Start

1. Initialize database configuration:

   ```
   dnn --db-init <database_type>
   ```

   Replace `<database_type>` with sqlite, mysql, mssql, or postgres.

   This will create a database.yaml that will be used by the framework to later connect with your db.

2. Generate context from your database:

   ```
   dnn --init
   ```

   This will prompt for a context name, you can give `product_analytics` or `customer_success` or any and it will then create YAML files in the `context/<contextname>` directory which will be your semantic layer for your data.
   You will be told to select couple of tables, so that it can be auto-labelled which you can edit later.

3. Or start an interactive chat session:

   ```
   dnn --chat <context_name>
   ```

   eg:

   ```
   dnn --chat product_analytics
   ```

   You can chat with the semantic layer that you have created. And you will also be able to save the metric
   to a dashboard, this will get created under `dashboards/<dashname>.yml`

4. You can generate reports with image as input for your dashboards. You need to have `wkhtmltopdf` in your system.
   For mac

```
brew install wkhtmltopdf
```

And then you need to install the dataneuron package with that dependency

```
pip install dataneuron[postgres, pdf]
```

Assuming you wanted both postgres and pdf.

```
dnn --report
```

## Configuration

Data Neuron supports various LLM providers. Set the following environment variables based on your chosen provider:

### Claude (Default)

```
CLAUDE_API_KEY=your_claude_api_key_here
```

### OpenAI

```
DATA_NEURON_LLM=openai
OPENAI_API_KEY=your_openai_api_key_here
OPENAI_MODEL=gpt-4  # Optional, defaults to gpt-4o
```

### Azure OpenAI

```
DATA_NEURON_LLM=azure
AZURE_OPENAI_API_KEY=your_azure_api_key_here
AZURE_OPENAI_API_VERSION=your_api_version_here
AZURE_OPENAI_ENDPOINT=your_azure_endpoint_here
AZURE_OPENAI_DEPLOYMENT_NAME=your_deployment_name_here
```

### Custom Provider

```
DATA_NEURON_LLM=custom
DATA_NEURON_LLM_API_KEY=your_custom_api_key_here
DATA_NEURON_LLM_ENDPOINT=your_custom_endpoint_here
DATA_NEURON_LLM_MODEL=your_preferred_model_here
```

### Ollama (for local LLM models)

Note: Doesn't generate good set of results.

```
DATA_NEURON_LLM=ollama
DATA_NEURON_LLM_MODEL=your_preferred_local_model_here
```

# Data Neuro package:

## Basic Usage

Here's a simple example of how to use DataNeuron:

```python
from dataneuron import DataNeuron

# Initialize DataNeuron
dn = DataNeuron(db_config='database.yaml', context='your_context_name')
dn.initialize()

# Ask a question
question = "How many users do we have?"
result = dn.query(question)

print(f"SQL Query: {result['sql']}")
print(f"Result: {result['result']}")
```

## Key Features

### 1. Initialization

The `DataNeuron` class needs to be initialized with a database configuration and a context:

```python
dn = DataNeuron(db_config='database.yaml', context='your_context_name', log=True)
dn.initialize()
```

- `db_config`: Path to your database configuration file or a dictionary with configuration details.
- `context`: Name of the context (semantic layer) you want to use or a dictionary with context details.
- `log`: Boolean to enable or disable logging (default is False).

### 2. Querying

You can use the `query` method to ask questions in natural language:

```python
result = dn.query("What are the top 5 products by sales?")
```

The `result` dictionary contains:

- `original_question`: The question you asked.
- `refined_question`: The question after refinement by the system.
- `sql`: The generated SQL query.
- `result`: The query results.
- `explanation`: An explanation of the query and results.

### 3. Chat Functionality

DataNeuron supports a chat-like interaction:

```python
sql, response = dn.chat("Who are our top customers?")
print(f"SQL: {sql}")
print(f"Response: {response}")
```

The `chat` method maintains a conversation history, allowing for context-aware follow-up questions.

### 4. Direct SQL Execution

You can execute SQL queries directly:

```python
result = dn.execute_query("SELECT * FROM users LIMIT 5")
```

### 5. Database Information

Retrieve information about your database:

```python
tables = dn.get_table_list()
table_info = dn.get_table_info("users")
```

### 6. Client/Tenant scoped queries/chat

First mark the client column in tables (important step). This will create a client_info.yaml that will be used
for lookup later for filtering the queries.

```
dnn --mc
```

Set the client context before querying or chatting

```
dn = DataNeuron(db_config='database.yaml', context='your_context')
dn.initialize()
dn.set_client_context(client_id)
result = dn.query("Your query here")
```

Every query that is generated will be filtered with client_id column based on the client column tables
that you had given earlier using `dnn --mc`, you can manually edit that file as well.

**Important Note on Limitations (WIP)**:

- Currently this client specific filter works on tables with client_id. For eg, if there is a
  scenario where you ask "My order items" but order_items table doesn't have client_id but the `orders` table,
  this won't add "JOIN" automatically yet.
- Similarly this won't work with Recursive CTE.

NOTE: All yaml files can be edited as long as the base structure is preserved, you can add any new columns
to tables yaml or definitions yaml, the structure involving name alone shouldn't be removed.

## Advanced Usage

### Setting Context

You can change the context during runtime:

```python
dn.set_context("new_context_name")
```

### Setting Chat History

For applications maintaining their own chat history:

```python
chat_history = [
    {"role": "user", "content": "How many orders do we have?"},
    {"role": "assistant", "content": "We have 1000 orders."}
]
dn.set_chat_history(chat_history)
```

### Error Handling

Always wrap DataNeuron calls in try-except blocks to handle potential errors:

```python
try:
    result = dn.query("What is our revenue this month?")
except ValueError as e:
    print(f"Error: {str(e)}")
```

## Integration Examples

### Web Server with Chat History

Here's an example of how to use DataNeuron in a web server application, maintaining chat history:

```python
from flask import Flask, request, jsonify
from dataneuron import DataNeuron

app = Flask(__name__)
dn = DataNeuron(db_config='database.yaml', context='default_context')
dn.initialize()

@app.route('/chat', methods=['POST'])
def chat():
    data = request.json
    messages = data.get('messages', [])
    context_name = data.get('context_name')

    if not messages or not isinstance(messages, list):
        return jsonify({"error": "messages must be a non-empty list"}), 400

    try:
        # Set chat history if there are previous messages
        if len(messages) > 1:
            dn.set_chat_history(messages[:-1])

        # Get the last user message
        last_user_message = next((msg['content'] for msg in reversed(messages) if msg['role'] == 'user'), None)

        if last_user_message is None:
            return jsonify({"error": "No user message found"}), 400

        sql, response = dn.chat(last_user_message)
        return jsonify({
            "response": response['data'],
            "sql": sql,
            "column_names": response['column_names']
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run()
```

### Generating HTML Reports

This example demonstrates how to use DataNeuron with DashboardManager to generate HTML reports:

```python
from flask import Flask, request, jsonify, Response
from dataneuron import DataNeuron, DashboardManager

app = Flask(__name__)

def get_dataneuron(context=None):
    dataneuron = DataNeuron(db_config='database.yaml', context=context)
    dataneuron.initialize()
    return dataneuron

def get_dashboard_manager():
    return DashboardManager()

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
        html_content = dm.generate_report_html(dashboard_name, instruction, image_path)
        return Response(html_content, mimetype='text/html')
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run()
```

# Data Neuron API

## API Endpoints

The Data Neuron API provides the following endpoints:

1. **Chat**

   - URL: `/chat`
   - Method: POST
   - Description: Process a chat message
   - Request Body:
     ```json
     {
       "messages": [{ "role": "user", "content": "Your message here" }],
       "context_name": "optional_context_name",
       "client_id": "optional_client_id"
     }
     ```

2. **Generate Report**

   - URL: `/reports`
   - Method: POST
   - Description: Generate an HTML report for a dashboard
   - Request Body:
     ```json
     {
       "dashboard_name": "your_dashboard_name",
       "instruction": "Instructions for report generation",
       "image_path": "optional/path/to/image.jpg"
     }
     ```

3. **List Dashboards**

   - URL: `/dashboards`
   - Method: GET
   - Description: Get a list of all available dashboards

4. **Get Dashboard Details**

   - URL: `/dashboards/<dashboard_id>`
   - Method: GET
   - Description: Get details of a specific dashboard

5. **Execute Metric**
   - URL: `/execute-metric`
   - Method: POST
   - Description: Execute a specific metric's SQL query
   - Request Body:
     ```json
     {
       "dashboard_id": "your_dashboard_id",
       "metric_name": "your_metric_name",
       "parameters": {
         "param1": "value1",
         "param2": "value2"
       }
     }
     ```

## Deployment Instructions

### Local Deployment

To run the Data Neuron API locally as a Flask server:

```bash
dnn --server [--host HOST] [--port PORT] [--debug]
```

### AWS Lambda Deployment

To deploy the Data Neuron API to AWS Lambda using Serverless Framework:

1. Ensure you have the Serverless Framework installed:

   ```bash
   npm install -g serverless
   ```

2. Install the required Serverless plugin:

   ```bash
   npm install --save-dev serverless-python-requirements
   ```

3. Create a `serverless.yml` file in your project root with the following content:

   ```yaml
   service: data-neuron-api

   provider:
     name: aws
     runtime: python3.8
     stage: ${opt:stage, 'dev'}
     region: ${opt:region, 'us-east-1'}

   functions:
     app:
       handler: dataneuron.lambda_handler
       events:
         - http:
             path: /{proxy+}
             method: ANY
       environment:
         CLAUDE_API_KEY: ${env:CLAUDE_API_KEY}
         # Add any other environment variables your app needs

   plugins:
     - serverless-python-requirements

   custom:
     pythonRequirements:
       dockerizePip: non-linux
   ```

4. Deploy your application:
   ```bash
   serverless deploy
   ```

After deployment, Serverless will output the API Gateway endpoint URL. You can use this URL to access your API.

Remember to set up any necessary environment variables and ensure your Lambda function has the appropriate permissions to access any required AWS services.

## Notes

- Make sure to handle authentication and authorization for your API endpoints in a production environment.
- When deploying to AWS Lambda, be aware of cold start times and the 15-minute execution limit for Lambda functions.
- Ensure your database is accessible from the Lambda function if your API requires database access.
- For large dependencies or if you exceed the Lambda package size limit, consider using Lambda Layers.

### Deployment to VPS or other

```
/path/to/your/python/env/bin/dnn --server --prod --host 0.0.0.0 --port 8080
```

You can mention the `--prod` so the flask server runs in a prod mode, you can mention other options as needed.

### Contributing

We welcome contributions to Data Neuron! Please see our [Contributing Guide](CONTRIBUTING.md) for more details on how to get started.

## Development

To set up Data Neuron for development:

1. Clone the repository:

   ```
   git clone https://github.com/databrainhq/dataneuron.git
   cd dataneuron
   ```

2. Install dependencies using Poetry:

   ```
   poetry install --all-extras
   ```

   or

   ```
   poetry install  --extras postgres

   ```

3. Run tests:
   ```
   poetry run pytest
   ```

Note: Tests are still being added.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contact

For questions, suggestions, or issues, please open an issue on the GitHub repository or contact the maintainers directly.

Happy querying with Data Neuron!

![neuron](https://github.com/user-attachments/assets/c48dbaf0-3ec9-4298-a3d0-af1c08bdb3be)
