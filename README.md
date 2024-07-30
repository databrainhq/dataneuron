# Data Neuron

Data Neuron is a powerful AI-driven data framework to create and maintain AI DATA analyst through CLI and as an API.

Use the API to let your business users chat with your Data through slack or teams or use for internally tools. Use the API to create dynamic feature rich (inspired by Claude Artifacts) html reports that you can send in email or slack.

Currently supports SQLite, PostgreSQL, MySQL, MSSQL, CSV files(through duckdb), Clickhouse. Works with major LLMs like Claude (default), OpenAI, LLAMA etc(through groq, nvidia, ..), OLLAMA.

## Quick Usage

- Install `pip install dataneuron[mssql, pdf]`
- Initialize database config: `dnn --db-init <database_type>`
- Generate context: `dnn --init`
- Start chat mode: `dnn --chat <contextname>` and save it as metrics to dashboards locally as yaml files.
- Get html pdf reports for your dashboard: `dnn --report`
- Run the API server to access chat, reports, dashboards, metrics: `dnn --server` (See API section for more)
- Deploy the server through AWS lambda or traditional VPS machine.
- If you have an existing Django or Flask or python project you can use this project as a library with functions like `process_chat` available.


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
       "context_name": "optional_context_name"
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
