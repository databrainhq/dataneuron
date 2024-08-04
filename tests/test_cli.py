import unittest
from unittest.mock import patch, MagicMock
from click.testing import CliRunner
from src.dataneuron.cli import main


class TestCLI(unittest.TestCase):
    def setUp(self):
        self.runner = CliRunner()

    @patch('os.path.exists')
    def test_cli_with_invalid_directory(self, mock_exists):
        mock_exists.return_value = False

        result = self.runner.invoke(main, ['--ask', 'total users count'])
        self.assertEqual(result.exit_code, 0)
        self.assertIn(
            "Error: Make sure you're in the correct directory", result.output)


if __name__ == '__main__':
    unittest.main()
