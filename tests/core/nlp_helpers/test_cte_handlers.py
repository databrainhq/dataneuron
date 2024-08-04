import unittest
import sqlparse
from dataneuron.core.nlp_helpers.cte_handler import extract_cte_definition, extract_main_query, filter_cte, handle_cte_query


class TestCTEHandler(unittest.TestCase):

    def setUp(self):
        self.mock_filter_function = lambda query, client_id: str(query).replace(
            'FROM', f'FROM (SELECT * FROM filtered_table WHERE client_id = {client_id}) AS filtered_subquery INNER JOIN'
        )

    def test_extract_cte_definition(self):
        query = '''
            WITH order_summary AS (
                SELECT user_id, COUNT(*) as order_count
                FROM orders
                GROUP BY user_id
            )
            SELECT * FROM order_summary
            '''
        parsed = sqlparse.parse(query)[0]

        print("\nAll tokens in parsed query:")
        for token in parsed.tokens:
            print(f"Type: {token.ttype}, Value: {repr(token.value)}")

        cte_part = extract_cte_definition(parsed)

        print("\nExtracted CTE part tokens:")
        for token in cte_part.tokens:
            print(f"Type: {token.ttype}, Value: {repr(token.value)}")

        print("\nExtracted CTE part as string:")
        print(str(cte_part))

        self.assertIn('WITH', str(cte_part))

    def test_extract_main_query(self):
        query = '''
        WITH order_summary AS (
            SELECT user_id, COUNT(*) as order_count
            FROM orders
            GROUP BY user_id
        )
        SELECT * FROM order_summary
        '''
        parsed = sqlparse.parse(query)[0]
        main_query = extract_main_query(parsed)
        self.assertIn('SELECT * FROM order_summary', str(main_query))
        self.assertNotIn('WITH', str(main_query))

    def test_filter_cte(self):
        cte_part = sqlparse.parse(
            'WITH order_summary AS (SELECT user_id, COUNT(*) as order_count FROM orders GROUP BY user_id)')[0]
        filtered_cte = filter_cte(cte_part, self.mock_filter_function, 1)
        self.assertIn('WITH order_summary AS', filtered_cte)
        self.assertIn(
            'FROM (SELECT * FROM filtered_table WHERE client_id = 1) AS filtered_subquery INNER JOIN', filtered_cte)

    def test_handle_cte_query_simple(self):
        query = '''
        WITH order_summary AS (
            SELECT user_id, COUNT(*) as order_count
            FROM orders
            GROUP BY user_id
        )
        SELECT * FROM order_summary
        '''
        parsed = sqlparse.parse(query)[0]
        result = handle_cte_query(parsed, self.mock_filter_function, 1)
        self.assertIn('WITH order_summary AS', result)
        self.assertIn(
            'FROM (SELECT * FROM filtered_table WHERE client_id = 1) AS filtered_subquery INNER JOIN', result)
        self.assertIn('SELECT * FROM', result)

    def test_handle_cte_query_complex(self):
        query = '''
        WITH
        order_summary AS (
            SELECT user_id, COUNT(*) as order_count
            FROM orders
            GROUP BY user_id
        ),
        high_value_users AS (
            SELECT user_id
            FROM order_summary
            WHERE order_count > 10
        )
        SELECT u.name, o.order_count
        FROM users u
        JOIN high_value_users hvu ON u.id = hvu.user_id
        JOIN order_summary o ON u.id = o.user_id
        '''
        parsed = sqlparse.parse(query)[0]
        result = handle_cte_query(parsed, self.mock_filter_function, 1)
        self.assertIn('WITH order_summary AS', result)
        self.assertIn('high_value_users AS', result)
        self.assertIn(
            'FROM (SELECT * FROM filtered_table WHERE client_id = 1) AS filtered_subquery INNER JOIN', result)
        self.assertIn('SELECT u.name, o.order_count', result)
