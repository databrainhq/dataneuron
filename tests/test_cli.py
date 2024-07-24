import unittest
from unittest.mock import patch, MagicMock
from click.testing import CliRunner
from src.data_neuron.cli import main


class TestCLI(unittest.TestCase):
    def setUp(self):
        self.runner = CliRunner()

    @patch('src.data_neuron.cli.load_context')
    @patch('src.data_neuron.cli.process_with_llm')
    @patch('src.data_neuron.cli.execute_query')
    @patch('os.path.exists')
    def test_cli_with_valid_input(self, mock_exists, mock_execute_query, mock_process_with_llm, mock_load_context):
        mock_exists.return_value = True  # Simulate that the required files/folders exist
        mock_load_context.return_value = {}
        mock_process_with_llm.return_value = "SELECT COUNT(*) FROM users"
        mock_execute_query.return_value = "10"

        result = self.runner.invoke(main, ['--ask', 'total users count'])
        self.assertEqual(result.exit_code, 0)
        self.assertIn('Query: SELECT COUNT(*) FROM users', result.output)
        self.assertIn('Result:\n10', result.output)

    @patch('os.path.exists')
    def test_cli_with_invalid_directory(self, mock_exists):
        mock_exists.return_value = False

        result = self.runner.invoke(main, ['--ask', 'total users count'])
        self.assertEqual(result.exit_code, 0)
        self.assertIn(
            "Error: Make sure you're in the correct directory", result.output)


if __name__ == '__main__':
    unittest.main()
