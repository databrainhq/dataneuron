import sqlite3
import unittest
from unittest.mock import patch, MagicMock
from data_neuron.query_executor import execute_query


class TestQueryExecutor(unittest.TestCase):
    @patch('sqlite3.connect')
    def test_execute_query(self, mock_connect):
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [(10,)]
        mock_connect.return_value = mock_connection

        result = execute_query("SELECT COUNT(*) FROM users")
        self.assertEqual(result, "(10,)")
        mock_cursor.execute.assert_called_once_with(
            "SELECT COUNT(*) FROM users")

    @patch('sqlite3.connect')
    def test_execute_query_sqlite_error(self, mock_connect):
        mock_connect.side_effect = sqlite3.Error("SQLite error")

        result = execute_query("SELECT COUNT(*) FROM users")
        self.assertEqual(result, "An error occurred: SQLite error")

    @patch('sqlite3.connect')
    def test_execute_query_general_error(self, mock_connect):
        mock_connect.side_effect = Exception("General error")

        result = execute_query("SELECT COUNT(*) FROM users")
        self.assertEqual(result, "An unexpected error occurred: General error")


if __name__ == '__main__':
    unittest.main()
