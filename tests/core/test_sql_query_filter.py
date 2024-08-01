import unittest
from data_neuron.core.sql_query_filter import SQLQueryFilter


class TestSQLQueryFilter(unittest.TestCase):
    def setUp(self):
        self.client_tables = {
            'main.orders': 'user_id',
            'orders': 'user_id',
            'main.products': 'company_id',
            'products': 'company_id',
            'inventory.items': 'organization_id',
            'items': 'organization_id'
        }
        self.filter = SQLQueryFilter(
            self.client_tables, schemas=['main', 'inventory'])

    def test_simple_query(self):
        query = 'SELECT * FROM orders'
        expected = 'SELECT * FROM orders WHERE "orders"."user_id" = 1'
        self.assertEqual(self.filter.apply_client_filter(query, 1), expected)

    def test_quoted_table_names(self):
        query = 'SELECT * FROM "orders"'
        expected = 'SELECT * FROM "orders" WHERE "orders"."user_id" = 1'
        self.assertEqual(self.filter.apply_client_filter(query, 1), expected)

    def test_schema_qualified_names(self):
        query = 'SELECT * FROM main.orders'
        expected = 'SELECT * FROM main.orders WHERE "main.orders"."user_id" = 1'
        self.assertEqual(self.filter.apply_client_filter(query, 1), expected)


if __name__ == '__main__':
    unittest.main()
