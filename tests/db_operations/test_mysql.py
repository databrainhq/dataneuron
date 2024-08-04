import unittest
from unittest.mock import patch, MagicMock
from dataneuron.db_operations.mysql import MySQLOperations


class TestMySQLOperations(unittest.TestCase):
    @patch('dataneuron.db_operations.mysql.connector')
    def setUp(self, mock_mysql):
        self.mock_conn = MagicMock()
        self.mock_cursor = MagicMock()
        mock_mysql.connect.return_value.__enter__.return_value = self.mock_conn
        self.mock_conn.cursor.return_value.__enter__.return_value = self.mock_cursor

        self.db = MySQLOperations('localhost', 'user', 'password', 'testdb')

    def test_execute_query(self):
        self.mock_cursor.fetchall.return_value = [('Alice',), ('Bob',)]
        result = self.db.execute_query("SELECT name FROM test_table")
        self.assertIn("Alice", result)
        self.assertIn("Bob", result)

    def test_get_schema_info(self):
        self.mock_cursor.fetchall.side_effect = [
            [('test_table',)],  # SHOW TABLES
            [('id', 'int(11)'), ('name', 'varchar(255)')]  # DESCRIBE test_table
        ]
        schema_info = self.db.get_schema_info()
        self.assertIn("Table: test_table", schema_info)
        self.assertIn("id (int(11))", schema_info)
        self.assertIn("name (varchar(255))", schema_info)
