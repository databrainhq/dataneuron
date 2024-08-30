import re
from sql_query_filter import SQLQueryFilter
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

    def test_simple_union(self):
        query = 'SELECT * FROM orders UNION SELECT * FROM products'
        expected = 'SELECT * FROM orders WHERE "orders"."user_id" = 1 UNION SELECT * FROM products WHERE "products"."company_id" = 1'
        self.assertEqual(self.filter.apply_client_filter(query, 1), expected)

    def test_union_with_existing_where(self):
        query = 'SELECT * FROM orders WHERE order_date > "2023-01-01" UNION SELECT * FROM products WHERE price > 100'
        expected = 'SELECT * FROM orders WHERE order_date > "2023-01-01" AND "orders"."user_id" = 1 UNION SELECT * FROM products WHERE price > 100 AND "products"."company_id" = 1'
        self.assertEqual(self.filter.apply_client_filter(query, 1), expected)

    def test_multiple_unions(self):
        query = 'SELECT * FROM orders UNION SELECT * FROM products UNION SELECT * FROM items'
        expected = 'SELECT * FROM orders WHERE "orders"."user_id" = 1 UNION SELECT * FROM products WHERE "products"."company_id" = 1 UNION SELECT * FROM items WHERE "items"."organization_id" = 1'
        self.assertEqual(self.filter.apply_client_filter(query, 1), expected)

    def test_union_all(self):
        query = 'SELECT * FROM orders UNION ALL SELECT * FROM products'
        expected = 'SELECT * FROM orders WHERE "orders"."user_id" = 1 UNION ALL SELECT * FROM products WHERE "products"."company_id" = 1'
        self.assertEqual(self.filter.apply_client_filter(query, 1), expected)

    def test_intersect(self):
        query = 'SELECT product_id FROM orders INTERSECT SELECT id FROM products'
        expected = 'SELECT product_id FROM orders WHERE "orders"."user_id" = 1 INTERSECT SELECT id FROM products WHERE "products"."company_id" = 1'
        self.assertEqual(self.filter.apply_client_filter(query, 1), expected)

    def test_except(self):
        query = 'SELECT product_id FROM orders EXCEPT SELECT id FROM products'
        expected = 'SELECT product_id FROM orders WHERE "orders"."user_id" = 1 EXCEPT SELECT id FROM products WHERE "products"."company_id" = 1'
        self.assertEqual(self.filter.apply_client_filter(query, 1), expected)

    def test_subquery_in_from(self):
        query = 'SELECT * FROM (SELECT * FROM orders) AS subq'
        expected = 'SELECT * FROM (SELECT * FROM orders WHERE "orders"."user_id" = 1) AS subq'
        self.assertEqual(self.filter.apply_client_filter(query, 1), expected)

    def test_subquery_in_join(self):
        query = 'SELECT o.* FROM orders o JOIN (SELECT * FROM products) p ON o.product_id = p.id'
        expected = 'SELECT o.* FROM orders o JOIN (SELECT * FROM products WHERE "products"."company_id" = 1) p ON o.product_id = p.id WHERE "o"."user_id" = 1'
        self.assertEqual(self.filter.apply_client_filter(query, 1), expected)

    def test_nested_subqueries(self):
        query = 'SELECT * FROM (SELECT * FROM (SELECT * FROM orders) AS inner_subq) AS outer_subq'
        expected = 'SELECT * FROM (SELECT * FROM (SELECT * FROM orders WHERE "orders"."user_id" = 1) AS inner_subq) AS outer_subq'
        self.assertEqual(self.filter.apply_client_filter(query, 1), expected)

    def test_subquery_in_where(self):
        query = 'SELECT * FROM orders WHERE product_id IN (SELECT id FROM products)'
        expected = 'SELECT * FROM orders WHERE product_id IN (SELECT id FROM products WHERE "products"."company_id" = 1) AND "orders"."user_id" = 1'
        self.assertEqual(self.filter.apply_client_filter(query, 1), expected)


class TestSQLQueryFilterCTE(unittest.TestCase):
    def setUp(self):
        self.client_tables = {
            'main.orders': 'user_id',
            'orders': 'user_id',
            'main.products': 'company_id',
            'products': 'company_id',
            'inventory.items': 'organization_id',
            'items': 'organization_id',
            'customers': 'customer_id',
            'categories': 'company_id'
        }
        self.filter = SQLQueryFilter(
            self.client_tables, schemas=['main', 'inventory'])

    def assertSQLEqual(self, first, second, msg=None):
        def normalize_sql(sql):
            # Remove all whitespace
            sql = re.sub(r'\s+', '', sql)
            # Convert to lowercase
            return sql.lower()

        normalized_first = normalize_sql(first)
        normalized_second = normalize_sql(second)
        self.assertEqual(normalized_first, normalized_second, msg)

    def test_simple_cte(self):
        query = '''
        WITH order_summary AS (
            SELECT user_id, COUNT(*) as order_count
            FROM orders
            GROUP BY user_id
        )
        SELECT * FROM order_summary
        '''
        expected = '''
        WITH order_summary AS (
            SELECT user_id, COUNT(*) as order_count
            FROM orders
            WHERE "orders"."user_id" = 1
            GROUP BY user_id
        )
        SELECT * FROM order_summary
        '''
        self.assertSQLEqual(
            self.filter.apply_client_filter(query, 1), expected)

    def test_cte_with_join(self):
        query = '''
        WITH high_value_orders AS (
            SELECT * FROM orders WHERE total_amount > 1000
        )
        SELECT c.name, hvo.order_id
        FROM customers c
        JOIN high_value_orders hvo ON c.id = hvo.user_id
        '''
        expected = '''
        WITH high_value_orders AS (
            SELECT * FROM orders WHERE total_amount > 1000 AND "orders"."user_id" = 1
        )
        SELECT c.name, hvo.order_id
        FROM customers c
        JOIN high_value_orders hvo ON c.id = hvo.user_id
        WHERE "c"."customer_id" = 1
        '''
        self.assertSQLEqual(
            self.filter.apply_client_filter(query, 1), expected)

    def test_multiple_ctes(self):
        query = '''
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
        '''
        expected = '''
        WITH order_summary AS (
            SELECT user_id, COUNT(*) as order_count
            FROM orders
            WHERE "orders"."user_id" = 1
            GROUP BY user_id
        ),
        product_summary AS (
            SELECT company_id, COUNT(*) as product_count
            FROM products
            WHERE "products"."company_id" = 1
            GROUP BY company_id
        )
        SELECT os.user_id, os.order_count, ps.product_count
        FROM order_summary os
        JOIN product_summary ps ON os.user_id = ps.company_id
        '''
        self.assertSQLEqual(
            self.filter.apply_client_filter(query, 1), expected)

    def test_cte_with_subquery(self):
        query = '''
        WITH top_products AS (
            SELECT p.id, p.name, SUM(o.quantity) as total_sold
            FROM products p
            JOIN (SELECT * FROM orders WHERE status = 'completed') o ON p.id = o.product_id
            GROUP BY p.id, p.name
            ORDER BY total_sold DESC
            LIMIT 10
        )
        SELECT * FROM top_products
        '''
        expected = '''
        WITH top_products AS (
            SELECT p.id, p.name, SUM(o.quantity) as total_sold
            FROM products p
            JOIN (SELECT * FROM orders WHERE status = 'completed' AND "orders"."user_id" = 1) o ON p.id = o.product_id
            WHERE "p"."company_id" = 1
            GROUP BY p.id, p.name
            ORDER BY total_sold DESC
            LIMIT 10
        )
        SELECT * FROM top_products
        '''
        self.assertSQLEqual(
            self.filter.apply_client_filter(query, 1), expected)

    def test_recursive_cte(self):
        query = '''
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
        '''
        expected = '''
        WITH RECURSIVE category_tree AS (
            SELECT id, name, parent_id, 0 AS level
            FROM categories
            WHERE parent_id IS NULL AND "categories"."company_id" = 1
            UNION ALL
            SELECT c.id, c.name, c.parent_id, ct.level + 1
            FROM categories c
            JOIN category_tree ct ON c.parent_id = ct.id
            WHERE "c"."company_id" = 1
        )
        SELECT * FROM category_tree
        '''
        self.assertSQLEqual(
            self.filter.apply_client_filter(query, 1), expected)

class TestSQLQueryFilterAdditional(unittest.TestCase):
    def setUp(self):
        self.client_tables = {
            'main.orders': 'user_id',
            'orders': 'user_id',
            'main.products': 'company_id',
            'products': 'company_id',
            'inventory.items': 'organization_id',
            'items': 'organization_id',
            'customers': 'customer_id',
            'categories': 'company_id'
        }
        self.filter = SQLQueryFilter(self.client_tables, schemas=['main', 'inventory'])

    def test_multiple_joins(self):
        query = 'SELECT o.id, p.name, c.email FROM orders o JOIN products p ON o.product_id = p.id JOIN customers c ON o.user_id = c.id'
        expected = 'SELECT o.id, p.name, c.email FROM orders o JOIN products p ON o.product_id = p.id JOIN customers c ON o.user_id = c.id WHERE "o"."user_id" = 1 AND "p"."company_id" = 1 AND "c"."customer_id" = 1'
        self.assertEqual(self.filter.apply_client_filter(query, 1), expected)

    def test_case_statement(self):
        query = 'SELECT id, CASE WHEN total_amount > 1000 THEN "High" ELSE "Low" END AS order_value FROM orders'
        expected = 'SELECT id, CASE WHEN total_amount > 1000 THEN "High" ELSE "Low" END AS order_value FROM orders WHERE "orders"."user_id" = 1'
        self.assertEqual(self.filter.apply_client_filter(query, 1), expected)

    def test_subquery_in_select(self):
        query = 'SELECT o.id, (SELECT COUNT(*) FROM products p WHERE p.id = o.product_id) AS product_count FROM orders o'
        expected = 'SELECT o.id, (SELECT COUNT(*) FROM products p WHERE p.id = o.product_id AND "p"."company_id" = 1) AS product_count FROM orders o WHERE "o"."user_id" = 1'
        self.assertEqual(self.filter.apply_client_filter(query, 1), expected)

    def test_having_clause(self):
        query = 'SELECT product_id, COUNT(*) FROM orders GROUP BY product_id HAVING COUNT(*) > 5'
        expected = 'SELECT product_id, COUNT(*) FROM orders WHERE "orders"."user_id" = 1 GROUP BY product_id HAVING COUNT(*) > 5'
        self.assertEqual(self.filter.apply_client_filter(query, 1), expected)

    def test_order_by_with_limit(self):
        query = 'SELECT * FROM orders ORDER BY total_amount DESC LIMIT 10'
        expected = 'SELECT * FROM orders WHERE "orders"."user_id" = 1 ORDER BY total_amount DESC LIMIT 10'
        self.assertEqual(self.filter.apply_client_filter(query, 1), expected)

    def test_union_with_order_by(self):
        query = 'SELECT id FROM orders UNION SELECT id FROM products ORDER BY id'
        expected = 'SELECT id FROM orders WHERE "orders"."user_id" = 1 UNION SELECT id FROM products WHERE "products"."company_id" = 1 ORDER BY id'
        self.assertEqual(self.filter.apply_client_filter(query, 1), expected)

    def test_subquery_with_aggregate(self):
        query = 'SELECT * FROM orders WHERE total_amount > (SELECT AVG(total_amount) FROM orders)'
        expected = 'SELECT * FROM orders WHERE total_amount > (SELECT AVG(total_amount) FROM orders WHERE "orders"."user_id" = 1) AND "orders"."user_id" = 1'
        self.assertEqual(self.filter.apply_client_filter(query, 1), expected)

    def test_complex_join_with_subquery(self):
        query = '''
        SELECT o.id, p.name 
        FROM orders o 
        JOIN (SELECT id, name FROM products WHERE price > 100) p ON o.product_id = p.id 
        WHERE o.status = 'completed'
        '''
        expected = '''
        SELECT o.id, p.name 
        FROM orders o 
        JOIN (SELECT id, name FROM products WHERE price > 100 AND "products"."company_id" = 1) p ON o.product_id = p.id 
        WHERE o.status = 'completed' AND "o"."user_id" = 1
        '''
        self.assertEqual(self.filter.apply_client_filter(query, 1), expected)

    def test_complex_nested_subqueries(self):
        query = '''
        SELECT *
        FROM orders o
        WHERE o.product_id IN (
            SELECT id
            FROM products
            WHERE category_id IN (
                SELECT id
                FROM categories
                WHERE name LIKE 'Electronics%'
            )
        ) AND o.user_id IN (
            SELECT user_id
            FROM (
                SELECT user_id, AVG(total_amount) as avg_order
                FROM orders
                GROUP BY user_id
                HAVING AVG(total_amount) > 1000
            ) high_value_customers
        )
        '''
        expected = '''
        SELECT *
        FROM orders o
        WHERE o.product_id IN (
            SELECT id
            FROM products
            WHERE category_id IN (
                SELECT id
                FROM categories
                WHERE name LIKE 'Electronics%'
                AND "categories"."company_id" = 1
            )
            AND "products"."company_id" = 1
        ) AND o.user_id IN (
            SELECT user_id
            FROM (
                SELECT user_id, AVG(total_amount) as avg_order
                FROM orders
                WHERE "orders"."user_id" = 1
                GROUP BY user_id
                HAVING AVG(total_amount) > 1000
            ) high_value_customers
        )
        AND "o"."user_id" = 1
        '''
        self.assertEqual(self.filter.apply_client_filter(query, 1), expected)

    def test_group_by_having_order_by(self):
        query = '''
        SELECT product_id, COUNT(*) as order_count, SUM(total_amount) as total_sales
        FROM orders
        GROUP BY product_id
        HAVING COUNT(*) > 10
        ORDER BY total_sales DESC
        LIMIT 5
        '''
        expected = '''
        SELECT product_id, COUNT(*) as order_count, SUM(total_amount) as total_sales
        FROM orders
        WHERE "orders"."user_id" = 1
        GROUP BY product_id
        HAVING COUNT(*) > 10
        ORDER BY total_sales DESC
        LIMIT 5
        '''
        self.assertEqual(self.filter.apply_client_filter(query, 1), expected)

    def test_different_data_types_in_where(self):
        query = '''
        SELECT *
        FROM orders
        WHERE order_date > '2023-01-01'
        AND total_amount > 100.50
        AND status IN ('completed', 'shipped')
        AND is_priority = TRUE
        '''
        expected = '''
        SELECT *
        FROM orders
        WHERE order_date > '2023-01-01'
        AND total_amount > 100.50
        AND status IN ('completed', 'shipped')
        AND is_priority = TRUE
        AND "orders"."user_id" = 1
        '''
        self.assertEqual(self.filter.apply_client_filter(query, 1), expected)

    def test_multi_schema_query(self):
        query = '''
        SELECT o.id, p.name, i.quantity
        FROM main.orders o
        JOIN main.products p ON o.product_id = p.id
        JOIN inventory.items i ON p.id = i.product_id
        '''
        expected = '''
        SELECT o.id, p.name, i.quantity
        FROM main.orders o
        JOIN main.products p ON o.product_id = p.id
        JOIN inventory.items i ON p.id = i.product_id
        WHERE "o"."user_id" = 1 AND "p"."company_id" = 1 AND "i"."organization_id" = 1
        '''
        self.assertEqual(self.filter.apply_client_filter(query, 1), expected)


class TestSQLQueryFilterAdditionalCTE(unittest.TestCase):
    def setUp(self):
        self.client_tables = {
            'main.orders': 'user_id',
            'orders': 'user_id',
            'main.products': 'company_id',
            'products': 'company_id',
            'inventory.items': 'organization_id',
            'items': 'organization_id',
            'customers': 'customer_id',
            'categories': 'company_id'
        }
        self.filter = SQLQueryFilter(
            self.client_tables, schemas=['main', 'inventory'])

    def assertSQLEqual(self, first, second, msg=None):
        def normalize_sql(sql):
            # Remove all whitespace
            sql = re.sub(r'\s+', '', sql)
            # Convert to lowercase
            return sql.lower()

        normalized_first = normalize_sql(first)
        normalized_second = normalize_sql(second)
        self.assertEqual(normalized_first, normalized_second, msg)

    def test_cte_with_union(self):
        query = '''
        WITH combined_data AS (
            SELECT id, 'order' AS type FROM orders
            UNION ALL
            SELECT id, 'product' AS type FROM products
        )
        SELECT * FROM combined_data
        '''
        expected = '''
        WITH combined_data AS (
            SELECT id, 'order' AS type FROM orders WHERE "orders"."user_id" = 1
            UNION ALL
            SELECT id, 'product' AS type FROM products WHERE "products"."company_id" = 1
        )
        SELECT * FROM combined_data
        '''
        self.assertEqual(self.filter.apply_client_filter(query, 1), expected)

    def test_set_operations_with_cte(self):
        query = '''
        WITH order_summary AS (
            SELECT user_id, COUNT(*) as order_count
            FROM orders
            GROUP BY user_id
        )
        SELECT * FROM order_summary
        UNION
        SELECT company_id as user_id, COUNT(*) as product_count
        FROM products
        GROUP BY company_id
        '''
        expected = '''
        WITH order_summary AS (
            SELECT user_id, COUNT(*) as order_count
            FROM orders
            WHERE "orders"."user_id" = 1
            GROUP BY user_id
        )
        SELECT * FROM order_summary
        UNION
        SELECT company_id as user_id, COUNT(*) as product_count
        FROM products
        WHERE "products"."company_id" = 1
        GROUP BY company_id
        '''
        self.assertSQLEqual(self.filter.apply_client_filter(query, 1), expected)

    def test_recursive_cte_with_join(self):
        query = '''
        WITH RECURSIVE category_tree AS (
            SELECT id, name, parent_id, 0 AS level
            FROM categories
            WHERE parent_id IS NULL
            UNION ALL
            SELECT c.id, c.name, c.parent_id, ct.level + 1
            FROM categories c
            JOIN category_tree ct ON c.parent_id = ct.id
        )
        SELECT ct.*, p.name as product_name
        FROM category_tree ct
        LEFT JOIN products p ON ct.id = p.category_id
        '''
        expected = '''
        WITH RECURSIVE category_tree AS (
            SELECT id, name, parent_id, 0 AS level
            FROM categories
            WHERE parent_id IS NULL AND "categories"."company_id" = 1
            UNION ALL
            SELECT c.id, c.name, c.parent_id, ct.level + 1
            FROM categories c
            JOIN category_tree ct ON c.parent_id = ct.id
            WHERE "c"."company_id" = 1
        )
        SELECT ct.*, p.name as product_name
        FROM category_tree ct
        LEFT JOIN products p ON ct.id = p.category_id
        WHERE "p"."company_id" = 1
        '''
        self.assertSQLEqual(self.filter.apply_client_filter(query, 1), expected)


if __name__ == '__main__':
    unittest.main()
