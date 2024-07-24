import unittest
from data_neuron.db_operations.factory import DatabaseFactory
from data_neuron.db_operations.sqlite import SQLiteOperations
from data_neuron.db_operations.postgres import PostgreSQLOperations
from data_neuron.db_operations.mysql import MySQLOperations


class TestDatabaseFactory(unittest.TestCase):
    def test_get_sqlite_database(self):
        db = DatabaseFactory.get_database('sqlite', db_path='test.db')
        self.assertIsInstance(db, SQLiteOperations)

    def test_get_postgres_database(self):
        db = DatabaseFactory.get_database(
            'postgres', dbname='testdb', user='user', password='pass', host='localhost', port='5432')
        self.assertIsInstance(db, PostgreSQLOperations)

    def test_get_mysql_database(self):
        db = DatabaseFactory.get_database(
            'mysql', host='localhost', user='user', password='pass', database='testdb')
        self.assertIsInstance(db, MySQLOperations)

    def test_invalid_database_type(self):
        with self.assertRaises(ValueError):
            DatabaseFactory.get_database('invalid_db')
