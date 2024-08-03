import unittest
from sqlparse import parse
from data_neuron.core.nlp_helpers.is_cte import is_cte_query as is_cte


class TestIsCTE(unittest.TestCase):
    def _assert_is_cte(self, query, expected):
        parsed = parse(query)[0]
        self.assertEqual(is_cte(parsed), expected)

    def test_simple_cte(self):
        query = "WITH cte AS (SELECT * FROM table) SELECT * FROM cte"
        self._assert_is_cte(query, True)

    def test_complex_cte_with_join(self):
        query = """
        WITH high_value_orders AS (
            SELECT * FROM orders WHERE total_amount > 1000
        )
        SELECT c.name, hvo.order_id
        FROM customers c
        JOIN high_value_orders hvo ON c.id = hvo.user_id
        """
        self._assert_is_cte(query, True)

    def test_multiple_ctes(self):
        query = """
        WITH cte1 AS (SELECT * FROM table1),
             cte2 AS (SELECT * FROM table2)
        SELECT * FROM cte1 JOIN cte2
        """
        self._assert_is_cte(query, True)

    def test_cte_with_subquery(self):
        query = """
        WITH cte AS (
            SELECT * FROM (SELECT * FROM inner_table) subquery
        )
        SELECT * FROM cte
        """
        self._assert_is_cte(query, True)

    def test_non_cte_query(self):
        query = "SELECT * FROM table"
        self._assert_is_cte(query, False)

    def test_with_in_column_name(self):
        query = "SELECT width FROM table"
        self._assert_is_cte(query, False)

    def test_with_in_string(self):
        query = "SELECT 'query with CTE' AS description FROM table"
        self._assert_is_cte(query, False)

    def test_cte_with_extra_whitespace(self):
        query = "\n\n  WITH    cte    AS    (SELECT 1) SELECT * FROM cte"
        self._assert_is_cte(query, True)

    def test_cte_with_comment_before(self):
        query = "/* Comment */ WITH cte AS (SELECT 1) SELECT * FROM cte"
        self._assert_is_cte(query, True)

    def test_cte_with_line_comment_before(self):
        query = "-- Comment\nWITH cte AS (SELECT 1) SELECT * FROM cte"
        self._assert_is_cte(query, True)

    def test_with_as_part_of_identifier(self):
        query = "WITHcte AS (SELECT 1) SELECT * FROM cte"
        self._assert_is_cte(query, False)

    # New test cases
    def test_cte_with_aggregation(self):
        query = """
        WITH order_summary AS (
            SELECT user_id, COUNT(*) as order_count
            FROM orders
            GROUP BY user_id
        )
        SELECT * FROM order_summary
        """
        self._assert_is_cte(query, True)

    def test_cte_with_join_and_filter(self):
        query = """
        WITH high_value_orders AS (
            SELECT * FROM orders WHERE total_amount > 1000
        )
        SELECT c.name, hvo.order_id
        FROM customers c
        JOIN high_value_orders hvo ON c.id = hvo.user_id
        """
        self._assert_is_cte(query, True)

    def test_multiple_ctes_with_join(self):
        query = """
        WITH order_summary AS (
            SELECT user_id, COUNT(*) as order_count
            FROM orders
            GROUP BY user_id
        ),
        product_summary AS (
            SELECT company_id, COUNT(*) as product_count
            FROM products
            GROUP BY company_id
        )
        SELECT os.user_id, os.order_count, ps.product_count
        FROM order_summary os
        JOIN product_summary ps ON os.user_id = ps.company_id
        """
        self._assert_is_cte(query, True)

    def test_cte_with_subquery_and_window_function(self):
        query = """
        WITH top_products AS (
            SELECT p.id, p.name, SUM(o.quantity) as total_sold
            FROM products p
            JOIN (SELECT * FROM orders WHERE status = 'completed') o ON p.id = o.product_id
            GROUP BY p.id, p.name
            ORDER BY total_sold DESC
            LIMIT 10
        )
        SELECT * FROM top_products
        """
        self._assert_is_cte(query, True)

    def test_recursive_cte(self):
        query = """
        WITH RECURSIVE category_tree AS (
            SELECT id, name, parent_id, 0 AS level
            FROM categories
            WHERE parent_id IS NULL
            UNION ALL
            SELECT c.id, c.name, c.parent_id, ct.level + 1
            FROM categories c
            JOIN category_tree ct ON c.parent_id = ct.id
        )
        SELECT * FROM category_tree
        """
        self._assert_is_cte(query, True)
