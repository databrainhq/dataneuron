import unittest
from unittest.mock import patch, MagicMock
from data_neuron.llm_processor import process_with_llm


class TestLLMProcessor(unittest.TestCase):
    @patch('openai.ChatCompletion.create')
    def test_process_with_llm(self, mock_openai):
        mock_response = MagicMock()
        mock_response.choices[0].message = {
            'content': 'SELECT COUNT(*) FROM users'}
        mock_openai.return_value = mock_response

        context = {
            'tables': {'users': {'table_name': 'users', 'columns': [{'name': 'id', 'type': 'INTEGER'}]}},
            'relationships': {},
            'global_definitions': {}
        }

        result = process_with_llm("How many users are there?", context)
        self.assertEqual(result, "SELECT COUNT(*) FROM users")
        mock_openai.assert_called_once()


if __name__ == '__main__':
    unittest.main()
