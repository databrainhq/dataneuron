import unittest
from unittest.mock import patch, MagicMock
from dataneuron.db_operations.mssql import MSSQLOperations


class TestMSSQLOperations(unittest.TestCase):
    @patch('dataneuron.db_operations.mssql.pyodbc')
    def setUp(self, mock_pyodbc):
        self.mock_conn = MagicMock()
        self.mock_cursor = MagicMock()
        mock_pyodbc.connect.return_value.__enter__.return_value = self.mock_conn
        self.mock_conn.cursor.return_value.__enter__.return_value = self.mock_cursor

        self.db = MSSQLOperations('localhost', 'testdb', 'user', 'password')

    def test_execute_query(self):
        self.mock_cursor.fetchall.return_value = [('Alice',), ('Bob',)]
        result = self.db.execute_query("SELECT name FROM test_table")
        self.assertIn("Alice", result)
        self.assertIn("Bob", result)

    def test_get_schema_info(self):
        self.mock_cursor.fetchall.return_value = [
            ('test_table', 'id', 'int'),
            ('test_table', 'name', 'varchar')
        ]
        schema_info = self.db.get_schema_info()
        self.assertIn("Table: test_table", schema_info)
        self.assertIn("id (int)", schema_info)
        self.assertIn("name (varchar)", schema_info)
