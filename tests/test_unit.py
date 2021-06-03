from unittest import TestCase

from sqlpt.sql import (Comparison, Field, FromClause, Join, Query,
                       SelectClause, Table, WhereClause)


class QueryTestCase(TestCase):
    def setUp(self):
        self.sql_str = ('select a name, b, fn(id, dob) age, fn(id, height) '
                        'from c join d on e = f where g = h and i = j')
        self.query = Query(self.sql_str)

    def test_fields(self):
        expected_fields = [
            Field(expression='a', alias='name'),
            Field(expression='b', alias=''),
            Field(expression='fn(id, dob)', alias='age'),
            Field(expression='fn(id, height)', alias=''),
        ]
        actual_fields = self.query.select_clause.fields

        self.assertEqual(actual_fields, expected_fields)

        self.assertEqual(str(actual_fields[0]), 'a name')
        self.assertEqual(str(actual_fields[1]), 'b')
        self.assertEqual(str(actual_fields[2]), 'fn(id, dob) age')
        self.assertEqual(str(actual_fields[3]), 'fn(id, height)')

    def test_select_clause(self):
        expected_select_clause_str = 'select a name, b, fn(id, dob) age, fn(id, height)'
        expected_select_clause = SelectClause(expected_select_clause_str)
        actual_select_clause = self.query.select_clause

        self.assertEqual(actual_select_clause, expected_select_clause)
        self.assertEqual(str(actual_select_clause), expected_select_clause_str)

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

        self.assertEqual(str(actual_joins[0]), 'c join d on e = f')

    def test_from_clause(self):
        expected_joins = [
            Join(
                left_table=Table(name='c'),
                right_table=Table(name='d'),
                comparisons=[Comparison(left_expression='e',
                                        operator='=',
                                        right_expression='f')]
            )
        ]

        expected_from_clause = FromClause(expected_joins)
        actual_from_clause = self.query.from_clause

        self.assertEqual(actual_from_clause, expected_from_clause)

        self.assertEqual(str(actual_from_clause), 'from c join d on e = f')

    def test_where_clause(self):
        comparisons = [
            Comparison(left_expression='g', operator='=', right_expression='h'),
            Comparison(left_expression='i', operator='=', right_expression='j'),
        ]
        expected_where_clause = WhereClause(comparisons)
        actual_where_clause = self.query.where_clause

        self.assertEqual(actual_where_clause, expected_where_clause)

        self.assertEqual(str(actual_where_clause), 'where g = h and i = j')

    def test_query(self):
        expected_query = Query(self.sql_str)
        actual_query = self.query

        self.assertEqual(actual_query, expected_query)

        self.assertEqual(str(actual_query), self.sql_str)


class ComplexQueryTestCase(TestCase):
    def setUp(self):
        self.sql_str = '''
            select a name,
                   b,
                   fn(id, dob) age,
                   fn(id, height),
                   (select c1 from a1 where a1.b1 = b) c1
              from c
              join d
                on e = f
              left
              join (select shape from k where kind = 'quadrilateral')
                on l = m
               and n = o
             where g = h
               and i = j
        '''

        self.query = Query(self.sql_str)

    def test_query(self):
        expected_query = Query(self.sql_str)
        actual_query = self.query

        self.assertEqual(actual_query, expected_query)

        actual_query_str = str(actual_query).replace(' ', '').replace('\n', '')
        expected_query_str = self.sql_str.replace(' ', '').replace('\n', '')

        self.assertEqual(actual_query_str, expected_query_str)


class EquivalenceTestCase(TestCase):
    def setUp(self):
        self.query_1 = Query(
            'select a, b from c join d on e = f where g = h and i = j')
        self.query_2 = Query(
            'select b, a from d join c on f = e where h = g and j = i')
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
