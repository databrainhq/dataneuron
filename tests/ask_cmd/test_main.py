import unittest
from unittest.mock import patch, MagicMock, call
import threading
from queue import Queue

# Assuming the query function is in a file named `data_neuron.ask_cmd.main.py`
from data_neuron.ask_cmd.main import query, process_with_llm


class TestQueryFunction(unittest.TestCase):

    @patch('data_neuron.ask_cmd.main.os.path.exists')
    @patch('data_neuron.ask_cmd.main.load_context')
    @patch('data_neuron.ask_cmd.main.process_with_llm')
    @patch('data_neuron.ask_cmd.main.click.echo')
    @patch('data_neuron.ask_cmd.main.print_header')
    @patch('data_neuron.ask_cmd.main.print_info')
    @patch('data_neuron.ask_cmd.main.print_success')
    @patch('data_neuron.ask_cmd.main.print_prompt')
    def test_query_success(
            self, mock_print_prompt, mock_print_success, mock_print_info,
            mock_print_header, mock_click_echo, mock_process_with_llm,
            mock_load_context, mock_path_exists):

        # Setup mocks
        mock_path_exists.side_effect = lambda x: x in [
            'database/sqlite.db', 'context']
        mock_load_context.return_value = 'mocked context'

        ask = "test question"
        query(ask)

        # Assertions to ensure the correct functions were called
        mock_print_header.assert_called_once_with("DATA neuron is thinking..")
        mock_print_info.assert_called_once_with(
            "🗄️ Fetching the context from your context folder")
        mock_print_success.assert_called_once_with("Context is loaded!\n")
        mock_print_prompt.assert_called_once_with("🤖 Sending request to LLM\n")
        mock_process_with_llm.assert_called_once_with(ask, 'mocked context')

    @patch('data_neuron.ask_cmd.main.os.path.exists')
    @patch('data_neuron.ask_cmd.main.click.echo')
    @patch('data_neuron.ask_cmd.main.print_header')
    def test_query_missing_files(self, mock_print_header, mock_click_echo, mock_path_exists):
        # Setup mocks
        mock_path_exists.side_effect = lambda x: x in ['context']
        ask = "test question"
        query(ask)

        # Ensure print_header is called
        mock_print_header.assert_called_once_with("DATA neuron is thinking..")

        # Ensure click.echo is called with the error message
        error_message = "Error: Make sure you're in the correct directory with 'database/sqlite.db' and 'context' folder."
        mock_click_echo.assert_any_call(error_message)


class TestProcessWithLLM(unittest.TestCase):

    @patch('data_neuron.ask_cmd.main.sql_query_prompt')
    @patch('data_neuron.ask_cmd.main.stream_neuron_api')
    @patch('data_neuron.ask_cmd.main.process_simplified_xml')
    @patch('data_neuron.ask_cmd.main.threading.Thread')
    def test_process_with_llm(self, mock_thread, mock_process_simplified_xml, mock_stream_neuron_api, mock_sql_query_prompt):
        # Setup mocks
        mock_sql_query_prompt.return_value = "mocked prompt"
        mock_stream_neuron_api.return_value = iter(["chunk1", "chunk2"])
        mock_thread_instance = MagicMock()
        mock_thread.return_value = mock_thread_instance

        query = "test query"
        context = {"key": "value"}

        process_with_llm(query, context)

        # Assertions
        mock_sql_query_prompt.assert_called_once_with(query, context)
        mock_stream_neuron_api.assert_called_once_with(
            "mocked prompt",
            instruction_prompt="You are a helpful assistant that generates SQL queries based on natural language questions."
        )
        mock_thread.assert_called_once()
        mock_thread_instance.start.assert_called_once()
        mock_thread_instance.join.assert_called_once()

        # Called for each chunk
        self.assertEqual(mock_process_simplified_xml.call_count, 2)

        # Check if the last item put in the queue was None (to signal the end)
        last_call = mock_process_simplified_xml.call_args_list[-1]
        self.assertIsNone(last_call[0][1]['sql_queue'].get())


if __name__ == '__main__':
    unittest.main()
