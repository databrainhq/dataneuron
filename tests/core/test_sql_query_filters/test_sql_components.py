import unittest
from sqlparse.sql import TokenList

from data_neuron.core.sql_query_filters.sql_components import SQLParserImplementation, TableExtractorImplementation
from data_neuron.core.sql_query_filters.where_clause_handler import WhereClauseModifierImplementation
from data_neuron.core.sql_query_filters.client_filter import ClientFilterApplierImplementation
from data_neuron.core.sql_query_filters.sub_query_handler import SubqueryHandlerImplementation, SetOperationHandlerImplementation
from data_neuron.core.sql_query_filters.main import SQLQueryFilter


class TestSQLParser(unittest.TestCase):
    def setUp(self):
        self.parser = SQLParserImplementation()

    def test_simple_query(self):
        query = "SELECT * FROM users"
        result = self.parser.parse(query)
        self.assertIsInstance(result, TokenList)
        self.assertEqual(str(result), query)

    def test_complex_query(self):
        query = "SELECT u.id, p.name FROM users u JOIN products p ON u.id = p.user_id WHERE u.active = TRUE"
        result = self.parser.parse(query)
        self.assertIsInstance(result, TokenList)
        self.assertEqual(str(result), query)


class TestTableExtractor(unittest.TestCase):
    def setUp(self):
        self.parser = SQLParserImplementation()
        self.extractor = TableExtractorImplementation()

    def test_simple_query(self):
        query = "SELECT * FROM users"
        parsed = self.parser.parse(query)
        result = self.extractor.extract_tables(parsed)
        expected = [{'name': 'users', 'schema': None, 'alias': None}]
        self.assertEqual(result, expected)

    def test_query_with_join(self):
        query = "SELECT u.id, p.name FROM users u JOIN products p ON u.id = p.user_id"
        parsed = self.parser.parse(query)
        result = self.extractor.extract_tables(parsed)
        expected = [
            {'name': 'users', 'schema': None, 'alias': 'u'},
            {'name': 'products', 'schema': None, 'alias': 'p'}
        ]
        self.assertEqual(result, expected)

    def test_query_with_schema(self):
        query = "SELECT * FROM public.users"
        parsed = self.parser.parse(query)
        result = self.extractor.extract_tables(parsed)
        expected = [{'name': 'users', 'schema': 'public', 'alias': None}]
        self.assertEqual(result, expected)

    def test_query_with_subquery(self):
        query = "SELECT * FROM (SELECT id FROM users) AS u"
        parsed = self.parser.parse(query)
        result = self.extractor.extract_tables(parsed)
        expected = [{'name': 'users', 'schema': None, 'alias': None}]
        self.assertEqual(result, expected)


class TestClientFilterApplier(unittest.TestCase):
    def setUp(self):
        self.client_tables = {
            'main.users': 'user_id',
            'users': 'user_id',
            'main.orders': 'client_id',
            'orders': 'client_id'
        }
        self.filter_applier = ClientFilterApplierImplementation(
            self.client_tables, schemas=['main'])

    def test_simple_table(self):
        table_info = {'name': 'users', 'schema': None, 'alias': None}
        result = self.filter_applier.apply_filter(table_info, 1)
        expected = '"users"."user_id" = 1'
        self.assertEqual(result, expected)

    def test_table_with_schema(self):
        table_info = {'name': 'users', 'schema': 'main', 'alias': None}
        result = self.filter_applier.apply_filter(table_info, 1)
        expected = '"main.users"."user_id" = 1'
        self.assertEqual(result, expected)

    def test_table_with_alias(self):
        table_info = {'name': 'users', 'schema': None, 'alias': 'u'}
        result = self.filter_applier.apply_filter(table_info, 1)
        expected = '"u"."user_id" = 1'
        self.assertEqual(result, expected)

    def test_unknown_table(self):
        table_info = {'name': 'unknown', 'schema': None, 'alias': None}
        result = self.filter_applier.apply_filter(table_info, 1)
        expected = ''
        self.assertEqual(result, expected)

    def test_case_insensitive_match(self):
        filter_applier = ClientFilterApplierImplementation(
            self.client_tables, schemas=['main'], case_sensitive=False)
        table_info = {'name': 'USERS', 'schema': None, 'alias': None}
        result = filter_applier.apply_filter(table_info, 1)
        expected = '"USERS"."user_id" = 1'
        self.assertEqual(result, expected)


class MockClientFilterApplier(ClientFilterApplierImplementation):
    def apply_filter(self, table_info, client_id):
        return f"{table_info['name']}_filter"


class TestSubqueryHandler(unittest.TestCase):
    def setUp(self):
        self.parser = SQLParserImplementation()
        self.filter_applier = MockClientFilterApplier({}, [])
        self.subquery_handler = SubqueryHandlerImplementation(
            self.parser, self.filter_applier)

    def test_simple_subquery(self):
        query = "(SELECT * FROM users)"
        parsed = self.parser.parse(query)
        result = self.subquery_handler.handle_subquery(parsed, 1)
        expected = "(SELECT * FROM users WHERE users_filter)"
        self.assertEqual(result, expected)

    def test_nested_subquery(self):
        query = "(SELECT * FROM (SELECT id FROM orders) AS o)"
        parsed = self.parser.parse(query)
        result = self.subquery_handler.handle_subquery(parsed, 1)
        expected = "(SELECT * FROM (SELECT id FROM orders WHERE orders_filter) AS o)"
        self.assertEqual(result, expected)


class TestSetOperationHandler(unittest.TestCase):
    def setUp(self):
        self.parser = SQLParserImplementation()
        self.filter_applier = MockClientFilterApplier({}, [])
        self.set_operation_handler = SetOperationHandlerImplementation(
            self.parser, self.filter_applier)

    def test_union(self):
        queries = ["SELECT * FROM users", "SELECT * FROM customers"]
        result = self.set_operation_handler.handle_set_operation(
            queries, "UNION", 1)
        expected = "SELECT * FROM users WHERE users_filter UNION SELECT * FROM customers WHERE customers_filter"
        self.assertEqual(result, expected)

    def test_intersect(self):
        queries = ["SELECT id FROM orders", "SELECT user_id FROM purchases"]
        result = self.set_operation_handler.handle_set_operation(
            queries, "INTERSECT", 1)
        expected = "SELECT id FROM orders WHERE orders_filter INTERSECT SELECT user_id FROM purchases WHERE purchases_filter"
        self.assertEqual(result, expected)


# Assuming the implementations are in a file called sql_components.py


class TestWhereClauseModifier(unittest.TestCase):
    def setUp(self):
        self.modifier = WhereClauseModifierImplementation()

    def test_add_where_clause(self):
        result = self.modifier.modify_where_clause(None, "user_id = 1")
        self.assertEqual(str(result), "WHERE user_id = 1")

    def test_modify_existing_where_clause(self):
        existing_where = Where("WHERE status = 'active'")
        result = self.modifier.modify_where_clause(
            existing_where, "user_id = 1")
        self.assertEqual(
            str(result), "WHERE status = 'active' AND user_id = 1")

    def test_modify_existing_where_token(self):
        existing_where = Token(Keyword, "WHERE created_at > '2023-01-01'")
        result = self.modifier.modify_where_clause(
            existing_where, "user_id = 1")
        self.assertEqual(
            str(result), "WHERE created_at > '2023-01-01' AND user_id = 1")


class TestSQLQueryFilterIntegration(unittest.TestCase):
    def setUp(self):
        client_tables = {
            'main.users': 'user_id',
            'users': 'user_id',
            'main.orders': 'client_id',
            'orders': 'client_id'
        }
        parser = SQLParserImplementation()
        extractor = TableExtractorImplementation()
        filter_applier = ClientFilterApplierImplementation(
            client_tables, schemas=['main'])
        subquery_handler = SubqueryHandlerImplementation(
            parser, filter_applier)
        set_operation_handler = SetOperationHandlerImplementation(
            parser, filter_applier)
        where_modifier = WhereClauseModifierImplementation()

        self.query_filter = SQLQueryFilter(
            parser, extractor, filter_applier, subquery_handler, set_operation_handler, where_modifier
        )

    def test_simple_query(self):
        query = "SELECT * FROM users"
        result = self.query_filter.apply_client_filter(query, 1)
        expected = 'SELECT * FROM users WHERE "users"."user_id" = 1'
        self.assertEqual(result, expected)

    def test_query_with_existing_where(self):
        query = "SELECT * FROM users WHERE status = 'active'"
        result = self.query_filter.apply_client_filter(query, 1)
        expected = 'SELECT * FROM users WHERE status = \'active\' AND "users"."user_id" = 1'
        self.assertEqual(result, expected)

    def test_query_with_join(self):
        query = "SELECT u.id, o.order_date FROM users u JOIN orders o ON u.id = o.user_id"
        result = self.query_filter.apply_client_filter(query, 1)
        expected = 'SELECT u.id, o.order_date FROM users u JOIN orders o ON u.id = o.user_id WHERE "u"."user_id" = 1 AND "o"."client_id" = 1'
        self.assertEqual(result, expected)

    def test_query_with_subquery(self):
        query = "SELECT * FROM (SELECT id FROM users) AS u"
        result = self.query_filter.apply_client_filter(query, 1)
        expected = 'SELECT * FROM (SELECT id FROM users WHERE "users"."user_id" = 1) AS u'
        self.assertEqual(result, expected)

    def test_query_with_union(self):
        query = "SELECT id FROM users UNION SELECT client_id FROM orders"
        result = self.query_filter.apply_client_filter(query, 1)
        expected = 'SELECT id FROM users WHERE "users"."user_id" = 1 UNION SELECT client_id FROM orders WHERE "orders"."client_id" = 1'
        self.assertEqual(result, expected)
