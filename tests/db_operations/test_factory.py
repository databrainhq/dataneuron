import unittest
from unittest.mock import patch, mock_open
import yaml
from dataneuron.db_operations.factory import DatabaseFactory
from dataneuron.db_operations.sqlite import SQLiteOperations
from dataneuron.db_operations.postgres import PostgreSQLOperations
from dataneuron.db_operations.mysql import MySQLOperations
from dataneuron.db_operations.mssql import MSSQLOperations


class TestDatabaseFactory(unittest.TestCase):

    @patch('os.path.exists')
    @patch('builtins.open', new_callable=mock_open, read_data=yaml.dump({
        'database': {
            'name': 'sqlite',
            'db_path': 'test.db'
        }
    }))
    def test_get_sqlite_database(self, mock_file, mock_exists):
        mock_exists.return_value = True
        db = DatabaseFactory.get_database()
        self.assertIsInstance(db, SQLiteOperations)

    @patch('os.path.exists')
    @patch('builtins.open', new_callable=mock_open, read_data=yaml.dump({
        'database': {
            'name': 'postgres',
            'dbname': 'testdb',
            'user': 'user',
            'password': 'pass',
            'host': 'localhost',
            'port': '5432'
        }
    }))
    def test_get_postgres_database(self, mock_file, mock_exists):
        mock_exists.return_value = True
        db = DatabaseFactory.get_database()
        self.assertIsInstance(db, PostgreSQLOperations)

    @patch('os.path.exists')
    @patch('builtins.open', new_callable=mock_open, read_data=yaml.dump({
        'database': {
            'name': 'mysql',
            'host': 'localhost',
            'user': 'user',
            'password': 'pass',
            'database': 'testdb'
        }
    }))
    def test_get_mysql_database(self, mock_file, mock_exists):
        mock_exists.return_value = True
        db = DatabaseFactory.get_database()
        self.assertIsInstance(db, MySQLOperations)

    @patch('os.path.exists')
    @patch('builtins.open', new_callable=mock_open, read_data=yaml.dump({
        'database': {
            'name': 'mssql',
            'server': 'localhost',
            'database': 'testdb',
            'username': 'user',
            'password': 'pass'
        }
    }))
    def test_get_mssql_database(self, mock_file, mock_exists):
        mock_exists.return_value = True
        db = DatabaseFactory.get_database()
        self.assertIsInstance(db, MSSQLOperations)

    @patch('os.path.exists')
    @patch('builtins.open', new_callable=mock_open, read_data=yaml.dump({
        'database': {
            'name': 'invalid_db'
        }
    }))
    def test_invalid_database_type(self, mock_file, mock_exists):
        mock_exists.return_value = True
        with self.assertRaises(ValueError):
            DatabaseFactory.get_database()

    @patch('os.path.exists')
    def test_missing_config_file(self, mock_exists):
        mock_exists.return_value = False
        with self.assertRaises(FileNotFoundError):
            DatabaseFactory.get_database()

    @patch('os.path.exists')
    @patch('builtins.open', new_callable=mock_open, read_data=yaml.dump({
        'database': {
            'name': 'sqlite',
            'db_path': 'test.db'
        }
    }))
    def test_load_config(self, mock_file, mock_exists):
        mock_exists.return_value = True
        config = DatabaseFactory.load_config()
        self.assertEqual(config, {'name': 'sqlite', 'db_path': 'test.db'})


if __name__ == '__main__':
    unittest.main()
