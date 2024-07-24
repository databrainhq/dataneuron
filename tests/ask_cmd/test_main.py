import unittest
from unittest.mock import patch, MagicMock, call
import os

# Assuming the query function is in a file named `data_neuron.ask_cmd.main.py`
from data_neuron.ask_cmd.main import query


class TestQueryFunction(unittest.TestCase):

    @patch('data_neuron.ask_cmd.main.os.path.exists')
    @patch('data_neuron.ask_cmd.main.load_context')
    @patch('data_neuron.ask_cmd.main.process_with_llm')
    @patch('data_neuron.ask_cmd.main.execute_query')
    @patch('data_neuron.ask_cmd.main.click.echo')
    @patch('data_neuron.ask_cmd.main.print_header')
    @patch('data_neuron.ask_cmd.main.print_info')
    @patch('data_neuron.ask_cmd.main.print_success')
    @patch('data_neuron.ask_cmd.main.print_prompt')
    @patch('data_neuron.ask_cmd.main.print_info_secondary')
    @patch('data_neuron.ask_cmd.main.create_box')
    def test_query_success(
            self, mock_create_box, mock_print_info_secondary, mock_print_prompt,
            mock_print_success, mock_print_info, mock_print_header, mock_click_echo,
            mock_execute_query, mock_process_with_llm, mock_load_context, mock_path_exists):

        # Setup mocks
        mock_path_exists.side_effect = lambda x: x in [
            'database/sqlite.db', 'context']
        mock_load_context.return_value = 'mocked context'
        mock_process_with_llm.return_value = 'SELECT * FROM test_table'
        mock_execute_query.return_value = 'mocked result'
        mock_create_box.return_value = 'mocked box'

        ask = "test question"

        with patch('builtins.print') as mock_print:
            query(ask)

        # Assertions to ensure the correct functions were called
        mock_print_header.assert_called_once_with("DATA neuron is thinking..")
        mock_print_info.assert_any_call(
            "🗄️ Fetching the context from your context folder")
        mock_print_success.assert_called_once_with("Context is loaded!\n")
        mock_print_prompt.assert_called_once_with("🤖 Sending request to LLM\n")
        mock_print_info_secondary.assert_called_once_with(
            "SQL query: SELECT * FROM test_table\n")
        mock_print_info.assert_any_call("⛏️ Executing the query..")
        mock_create_box.assert_called_once_with("Result", 'mocked result', '')

        # Ensure the box is printed
        mock_print.assert_called_once_with('mocked box')

    @patch('data_neuron.ask_cmd.main.os.path.exists')
    @patch('data_neuron.ask_cmd.main.click.echo')
    def test_query_missing_files(self, mock_click_echo, mock_path_exists):

        # Setup mocks
        mock_path_exists.side_effect = lambda x: x in ['context']

        ask = "test question"

        with patch('builtins.print') as mock_print:
            query(ask)

        # Ensure click.echo is called with the error message
        error_message = "Error: Make sure you're in the correct directory with 'database/sqlite.db' and 'context' folder."
        self.assertIn(
            call(error_message),
            mock_click_echo.mock_calls
        )

        # Ensure no further processing occurs
        mock_print.assert_not_called()


if __name__ == '__main__':
    unittest.main()
