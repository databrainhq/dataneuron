import unittest
from unittest.mock import patch, MagicMock
from dataneuron.db_operations.postgres import PostgreSQLOperations


class TestPostgreSQLOperations(unittest.TestCase):
    @patch('dataneuron.db_operations.postgres.psycopg2')
    def setUp(self, mock_psycopg2):
        self.mock_conn = MagicMock()
        self.mock_cursor = MagicMock()
        mock_psycopg2.connect.return_value.__enter__.return_value = self.mock_conn
        self.mock_conn.cursor.return_value.__enter__.return_value = self.mock_cursor

        self.db = PostgreSQLOperations(
            'testdb', 'user', 'password', 'localhost', '5432')

    def test_execute_query(self):
        self.mock_cursor.fetchall.return_value = [('Alice',), ('Bob',)]
        result = self.db.execute_query("SELECT name FROM test_table")
        self.assertIn("Alice", result)
        self.assertIn("Bob", result)

    def test_get_schema_info(self):
        self.mock_cursor.fetchall.return_value = [
            ('test_table', 'id', 'integer'),
            ('test_table', 'name', 'text')
        ]
        schema_info = self.db.get_schema_info()
        self.assertIn("Table: test_table", schema_info)
        self.assertIn("id (integer)", schema_info)
        self.assertIn("name (text)", schema_info)
