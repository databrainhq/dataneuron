import unittest
from sqlparse.sql import Token, Where, Parenthesis
from sqlparse.tokens import Keyword
from data_neuron.core.sql_query_filters.where_clause_handler import WhereClauseModifierImplementation
from data_neuron.core.sql_query_filters.client_filter import ClientFilterApplierImplementation
from data_neuron.core.sql_query_filters.sub_query_handler import SubqueryHandlerImplementation, SetOperationHandlerImplementation
from data_neuron.core.sql_query_filters.main import SQLQueryFilter
from data_neuron.core.sql_query_filters.sql_components import SQLParserImplementation, TableExtractorImplementation


# Assuming the implementations are in a file called sql_components.py


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
