import pytest
from data_neuron.llm_processor import process_with_llm


def test_process_with_llm(mocker):
    # Mock the OpenAI API call
    mock_openai = mocker.patch('openai.ChatCompletion.create')
    mock_openai.return_value.choices[0].message = {
        'content': 'SELECT COUNT(*) FROM users'}

    context = {
        'tables': {'users': {'table_name': 'users', 'columns': [{'name': 'id', 'type': 'INTEGER'}]}},
        'relationships': {},
        'global_definitions': {}
    }

    result = process_with_llm("How many users are there?", context)
    assert result == "SELECT COUNT(*) FROM users"
    mock_openai.assert_called_once()
