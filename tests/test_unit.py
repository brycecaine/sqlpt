from unittest import TestCase

from sqlpt.sql import Comparison, Field, Join, Query, SelectClause, Table


class QueryTestCase(TestCase):
    def setUp(self):
        self.query = Query(
            'select a name, b, fn(id, dob) age, fn(id, height) from c join d on e = f where g = h and i = j',
            'mock_db_str')

    def test_fields(self):
        expected_fields = [
            Field(expression='a', alias='name'),
            Field(expression='b', alias=''),
            Field(expression='fn(id, dob)', alias='age'),
            Field(expression='fn(id, height)', alias=''),
        ]
        actual_fields = self.query.select_clause.fields

        self.assertEqual(actual_fields, expected_fields)

    def test_joins(self):
        expected_joins = [
            Join(
                left_table=Table(name='c'),
                right_table=Table(name='d'),
                comparisons=[Comparison(left_expression='e',
                                        operator='=',
                                        right_expression='f')]
            )
        ]

        actual_joins = self.query.from_clause.joins

        self.assertEqual(actual_joins, expected_joins)


class EquivalenceTestCase(TestCase):
    def setUp(self):
        self.query_1 = Query(
            'select a, b from c join d on e = f where g = h and i = j',
            'mock_db_str')
        self.query_2 = Query(
            'select b, a from d join c on f = e where h = g and j = i',
            'mock_db_str')
        self.select_clause_1 = SelectClause('select a, b')
        self.select_clause_2 = SelectClause('select b, a')

    def test_select_clause_equivalence_1(self):
        equivalent_1 = self.select_clause_1.is_equivalent_to(
            self.select_clause_2)

        self.assertTrue(equivalent_1)

    def test_select_clause_equivalence_2(self):
        equivalent_2 = self.select_clause_2.is_equivalent_to(
            self.select_clause_1)

        self.assertTrue(equivalent_2)

    def test_query_select_clause_equivalence_1(self):
        equivalent_1 = self.query_1.select_clause.is_equivalent_to(
            self.query_2.select_clause)

        self.assertTrue(equivalent_1)

    def test_query_select_clause_equivalence_2(self):
        equivalent_2 = self.query_2.select_clause.is_equivalent_to(
            self.query_1.select_clause)

        self.assertTrue(equivalent_2)

    def test_query_from_clause_equivalence_1(self):
        equivalent_1 = self.query_1.from_clause.is_equivalent_to(
            self.query_2.from_clause)

        self.assertTrue(equivalent_1)

    def test_query_from_clause_equivalence_2(self):
        equivalent_2 = self.query_2.from_clause.is_equivalent_to(
            self.query_1.from_clause)

        self.assertTrue(equivalent_2)
