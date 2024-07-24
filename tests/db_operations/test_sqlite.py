import unittest
import os
from data_neuron.db_operations.sqlite import SQLiteOperations


class TestSQLiteOperations(unittest.TestCase):
    def setUp(self):
        self.db_path = 'test_sqlite.db'
        self.db = SQLiteOperations(self.db_path)

        # Create a test table
        self.db.execute_query("""
            CREATE TABLE IF NOT EXISTS test_table (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            )
        """)

        # Insert some test data
        self.db.execute_query("""
            INSERT INTO test_table (name) VALUES ('Alice'), ('Bob')
        """)

    def tearDown(self):
        os.remove(self.db_path)

    def test_execute_query(self):
        result = self.db.execute_query("SELECT * FROM test_table")
        self.assertIn("Alice", result)
        self.assertIn("Bob", result)

    def test_get_schema_info(self):
        schema_info = self.db.get_schema_info()
        self.assertIn("Table: test_table", schema_info)
        self.assertIn("id (INTEGER)", schema_info)
        self.assertIn("name (TEXT)", schema_info)
