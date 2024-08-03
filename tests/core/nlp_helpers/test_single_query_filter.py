import unittest
from data_neuron.core.nlp_helpers.single_query_filter import SingleQueryFilter


class TestSingleQueryFilter(unittest.TestCase):
    def setUp(self):
        client_tables = {
            'orders': 'user_id',
            'products': 'company_id',
            'customers': 'id',
        }
        self.filter_instance = SingleQueryFilter(client_tables)

    def test_simple_query(self):
        query = "SELECT * FROM orders"
        expected = 'SELECT * FROM orders WHERE "orders"."user_id" = 1'
        self.assertEqual(
            self.filter_instance.apply_filter_to_single_query(query, 1), expected)

    def test_query_with_where_clause(self):
        query = "SELECT * FROM orders WHERE status = 'completed'"
        expected = 'SELECT * FROM orders WHERE status = \'completed\' AND "orders"."user_id" = 1'
        self.assertEqual(
            self.filter_instance.apply_filter_to_single_query(query, 1), expected)

    def test_query_with_join(self):
        query = """
        SELECT o.id, p.name
        FROM orders o
        JOIN products p ON o.product_id = p.id
        """
        expected = """
        SELECT o.id, p.name
        FROM orders o
        JOIN products p ON o.product_id = p.id
        WHERE "o"."user_id" = 1 AND "p"."company_id" = 1
        """
        self.assertEqual(self.filter_instance.apply_filter_to_single_query(
            query, 1).strip(), expected.strip())

    def test_query_with_multiple_tables(self):
        query = """
        SELECT o.id, c.name, p.description
        FROM orders o
        JOIN customers c ON o.customer_id = c.id
        JOIN products p ON o.product_id = p.id
        """
        expected = """
        SELECT o.id, c.name, p.description
        FROM orders o
        JOIN customers c ON o.customer_id = c.id
        JOIN products p ON o.product_id = p.id
        WHERE "o"."user_id" = 1 AND "c"."id" = 1 AND "p"."company_id" = 1
        """
        self.assertEqual(self.filter_instance.apply_filter_to_single_query(
            query, 1).strip(), expected.strip())

    def test_query_with_group_by(self):
        query = """
        SELECT product_id, COUNT(*) as order_count
        FROM orders
        GROUP BY product_id
        """
        expected = """
        SELECT product_id, COUNT(*) as order_count
        FROM orders
        WHERE "orders"."user_id" = 1
        GROUP BY product_id
        """
        self.assertEqual(self.filter_instance.apply_filter_to_single_query(
            query, 1).strip(), expected.strip())

    def test_query_with_order_by(self):
        query = """
        SELECT * FROM products
        ORDER BY price DESC
        """
        expected = """
        SELECT * FROM products
        WHERE "products"."company_id" = 1
        ORDER BY price DESC
        """
        self.assertEqual(self.filter_instance.apply_filter_to_single_query(
            query, 1).strip(), expected.strip())

    def test_query_with_subquery_in_join(self):
        query = """
        SELECT p.id, p.name, SUM(o.quantity) as total_sold
        FROM products p
        JOIN (SELECT * FROM orders WHERE status = 'completed') o ON p.id = o.product_id
        GROUP BY p.id, p.name
        """
        expected = """
        SELECT p.id, p.name, SUM(o.quantity) as total_sold
        FROM products p
        JOIN (SELECT * FROM orders WHERE status = 'completed' AND "orders"."user_id" = 1) o ON p.id = o.product_id
        WHERE "p"."company_id" = 1
        GROUP BY p.id, p.name
        """
        result = self.filter_instance.apply_filter_to_single_query(query, 1)
        self.assertEqual(result.strip(), expected.strip())


if __name__ == '__main__':
    unittest.main()
