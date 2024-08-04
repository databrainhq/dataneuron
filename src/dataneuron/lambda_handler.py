import json
from .server import create_app


def lambda_handler(event, context):
    application = create_app()
    with application.test_client() as client:
        http_method = event['httpMethod']
        path = event['path']
        headers = event.get('headers', {})
        query_string = event.get('queryStringParameters', {})
        body = event.get('body', '')

        if body:
            body = json.loads(body)

        response = client.open(
            path,
            method=http_method,
            headers=headers,
            query_string=query_string,
            json=body
        )

        return {
            'statusCode': response.status_code,
            'body': response.data.decode('utf-8'),
            'headers': dict(response.headers)
        }
