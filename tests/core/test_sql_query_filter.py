from data_neuron.core.sql_query_filter import SQLQueryFilter
import unittest


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

    def test_table_alias(self):
        query = 'SELECT o.* FROM orders o'
        expected = 'SELECT o.* FROM orders o WHERE "o"."user_id" = 1'
        self.assertEqual(self.filter.apply_client_filter(query, 1), expected)

    def test_join(self):
        query = 'SELECT o.id, p.name FROM orders o JOIN products p ON o.product_id = p.id'
        expected = 'SELECT o.id, p.name FROM orders o JOIN products p ON o.product_id = p.id WHERE "o"."user_id" = 1 AND "p"."company_id" = 1'
        self.assertEqual(self.filter.apply_client_filter(query, 1), expected)

    def test_existing_where_clause(self):
        query = 'SELECT * FROM orders WHERE order_date > "2023-01-01"'
        expected = 'SELECT * FROM orders WHERE order_date > "2023-01-01" AND "orders"."user_id" = 1'
        result = self.filter.apply_client_filter(query, 1)
        print(f"Result: {result}")
        self.assertEqual(result, expected)

    def test_existing_where_clause_with_parentheses(self):
        query = 'SELECT * FROM orders WHERE (order_date > "2023-01-01" OR total_amount > 1000)'
        expected = 'SELECT * FROM orders WHERE (order_date > "2023-01-01" OR total_amount > 1000) AND "orders"."user_id" = 1'
        self.assertEqual(self.filter.apply_client_filter(query, 1), expected)

    def test_existing_where_clause_with_join(self):
        query = 'SELECT o.id, p.name FROM orders o JOIN products p ON o.product_id = p.id WHERE o.order_date > "2023-01-01"'
        expected = 'SELECT o.id, p.name FROM orders o JOIN products p ON o.product_id = p.id WHERE o.order_date > "2023-01-01" AND "o"."user_id" = 1 AND "p"."company_id" = 1'
        self.assertEqual(self.filter.apply_client_filter(query, 1), expected)

    def test_existing_where_clause_followed_by_group_by(self):
        query = 'SELECT product_id, COUNT(*) FROM orders WHERE order_date > "2023-01-01" GROUP BY product_id'
        expected = 'SELECT product_id, COUNT(*) FROM orders WHERE order_date > "2023-01-01" AND "orders"."user_id" = 1 GROUP BY product_id'
        self.assertEqual(self.filter.apply_client_filter(query, 1), expected)


if __name__ == '__main__':
    unittest.main()
