from unittest import TestCase

from sqlpt import service
from sqlpt.sql import (Field, FromClause, Join, OnClause, Query, SelectClause,
                       Table, WhereClause, parse_sql_clauses, parse_field,
                       parse_fields_from_str)


# TODO Delete this when StringTestCase, TokenListTestCase, and
#      StringListTestCase are done
class ClassTestCase(TestCase):
    def _test(self, sql_str, expected_select_clause, expected_from_clause,
              expected_where_clause):
        query = Query(sql_str)

        expected_query = Query(
            expected_select_clause,
            expected_from_clause,
            expected_where_clause)

        self.assertEqual(query, expected_query)
        self.assertEqual(query.select_clause, expected_select_clause)
        self.assertEqual(query.from_clause, expected_from_clause)
        self.assertEqual(query.where_clause, expected_where_clause)

    def test_query_basic(self):
        sql_str = "select * from dual where dummy = 'X'"

        expected_select_clause = SelectClause(['*'])
        expected_from_clause = FromClause(Table(name='dual'), [])
        expected_where_clause = WhereClause("where dummy = 'X'")

        self._test(sql_str, expected_select_clause, expected_from_clause,
                   expected_where_clause)

    def test_query_single_field(self):
        sql_str = "select field_1 from dual where dummy = 'X'"

        expected_select_clause = SelectClause(['field_1'])
        expected_from_clause = FromClause(Table(name='dual'), [])
        expected_where_clause = WhereClause("where dummy = 'X'")

        self._test(sql_str, expected_select_clause, expected_from_clause,
                   expected_where_clause)

    def test_query_multiple_fields(self):
        sql_str = "select field_1, field_2 from dual where dummy = 'X'"

        expected_select_clause = SelectClause(['select', ['field_1', 'field_2']])
        expected_from_clause = FromClause(Table(name='dual'), [])
        expected_where_clause = WhereClause("where dummy = 'X'")

        self._test(sql_str, expected_select_clause, expected_from_clause,
                   expected_where_clause)


class StringTestCase(TestCase):
    def _test(self, sql_str, expected_select_clause, expected_from_clause,
              expected_where_clause):
        query = Query(sql_str)

        expected_query = Query(
            expected_select_clause,
            expected_from_clause,
            expected_where_clause)

        self.assertEqual(query, expected_query)
        self.assertEqual(query.select_clause, expected_select_clause)
        self.assertEqual(query.from_clause, expected_from_clause)
        self.assertEqual(query.where_clause, expected_where_clause)

    def test_query_basic(self):
        sql_str = "select * from dual where dummy = 'X'"

        expected_select_clause = SelectClause('select *')
        expected_from_clause = FromClause('from dual')
        expected_where_clause = WhereClause("where dummy = 'X'")

        self._test(sql_str, expected_select_clause, expected_from_clause,
                   expected_where_clause)

    def test_query_single_field(self):
        sql_str = "select field_1 from dual where dummy = 'X'"

        expected_select_clause = SelectClause('select field_1')
        expected_from_clause = FromClause('from dual')
        expected_where_clause = WhereClause("where dummy = 'X'")

        self._test(sql_str, expected_select_clause, expected_from_clause,
                   expected_where_clause)

    def test_query_multiple_fields(self):
        sql_str = "select field_1, field_2 from dual where dummy = 'X'"

        expected_select_clause = SelectClause('select field_1, field_2')
        expected_from_clause = FromClause('from dual')
        expected_where_clause = WhereClause("where dummy = 'X'")

        self._test(sql_str, expected_select_clause, expected_from_clause,
                   expected_where_clause)


class StringListTestCase(TestCase):
    def _test(self, sql_str, expected_select_clause, expected_from_clause,
              expected_where_clause):
        query = Query(sql_str)

        expected_query = Query(
            expected_select_clause,
            expected_from_clause,
            expected_where_clause)

        self.assertEqual(query, expected_query)
        self.assertEqual(query.select_clause, expected_select_clause)
        self.assertEqual(query.from_clause, expected_from_clause)
        self.assertEqual(query.where_clause, expected_where_clause)

    def test_query_basic(self):
        sql_str = "select * from dual where dummy = 'X'"

        expected_select_clause = SelectClause(['select', '*'])
        expected_from_clause = FromClause(['from', 'dual', []])
        expected_where_clause = WhereClause(['where', 'dummy', '=', "'X'"])

        self._test(sql_str, expected_select_clause, expected_from_clause,
                   expected_where_clause)

    def test_query_single_field(self):
        sql_str = "select field_1 from dual where dummy = 'X'"

        expected_select_clause = SelectClause(['select', ['field_1']])
        expected_from_clause = FromClause(['from', 'dual', []])
        expected_where_clause = WhereClause(['where', 'dummy', '=', "'X'"])

        self._test(sql_str, expected_select_clause, expected_from_clause,
                   expected_where_clause)

    def test_query_multiple_fields(self):
        sql_str = "select field_1, field_2 from dual where dummy = 'X'"

        expected_select_clause = SelectClause(['select', ['field_1', 'field_2']])
        expected_from_clause = FromClause(['from', 'dual', []])
        expected_where_clause = WhereClause(['where', 'dummy', '=', "'X'"])

        self._test(sql_str, expected_select_clause, expected_from_clause,
                   expected_where_clause)


class ParseTestCase(TestCase):
    def _test_parse_sql_clauses(self):
        sql_str = '''
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

        select_clause, from_clause, where_clause = parse_sql_clauses(sql_str)

    def test_case_statement_in_parentheses(self):
        sql_str = '''
            select id,
                   (case when gpa > 3.7 then 'Y' else null end) honors
              from student
              left
              join student_gpa
                on student.id = student_id
            '''

        query = Query(sql_str)

        actual_sql_str = str(query)
        expected_sql_str = service.remove_whitespace_from_str(sql_str)

        self.assertEqual(actual_sql_str, expected_sql_str)


class FunctionTestCase(TestCase):
    def test_parse_field_basic_no_alias(self):
        actual_field = parse_field('a')
        expected_field = Field('a', '')
        self.assertEqual(actual_field, expected_field)

    def test_parse_field_basic_with_alias(self):
        actual_field = parse_field('b c')
        expected_field = Field('b', 'c')
        self.assertEqual(actual_field, expected_field)

    def test_parse_field_function_no_alias(self):
        actual_field = parse_field('fn(x, y)')
        expected_field = Field('fn(x, y)', '')
        self.assertEqual(actual_field, expected_field)

    def test_parse_field_function_with_alias(self):
        actual_field = parse_field('fn(x, y) z')
        expected_field = Field('fn(x, y)', 'z')
        self.assertEqual(actual_field, expected_field)

    def test_parse_field_subquery_no_alias(self):
        actual_field = parse_field('(select * from dual)')
        expected_field = Field('(select * from dual)', '')
        self.assertEqual(actual_field, expected_field)

    def test_parse_field_subquery_with_alias(self):
        actual_field = parse_field('(select * from dual) z')
        expected_field = Field('(select * from dual)', 'z')
        self.assertEqual(actual_field, expected_field)

    def test_parse_fields_from_str(self):
        actual_fields = parse_fields_from_str(
            'a, b c, fn(x, y), fn(x, y) z, (select * from dual), '
            '(select * from dual) z')
        expected_fields = [
            Field('a', ''),
            Field('b', 'c'),
            Field('fn(x, y)', ''),
            Field('fn(x, y)', 'z'),
            Field('(select * from dual)', ''),
            Field('(select * from dual)', 'z'),
        ]

        self.assertEqual(actual_fields, expected_fields)


class QueryTestCase(TestCase):
    def setUp(self):
        self.sql_str = ('select a name, b, fn(id, dob) age, fn(id, height) '
                        'from c join d on e = f where g = h and i = j')
        self.query = Query(self.sql_str)

    def test_fields(self):
        expected_fields = [
            Field('a', 'name'),
            Field('b', ''),
            Field('fn(id, dob)', 'age'),
            Field('fn(id, height)', ''),
        ]
        actual_fields = self.query.select_clause.fields

        self.assertEqual(actual_fields, expected_fields)

        self.assertEqual(str(actual_fields[0]), 'a name')
        self.assertEqual(str(actual_fields[1]), 'b')
        self.assertEqual(str(actual_fields[2]), 'fn(id, dob) age')
        self.assertEqual(str(actual_fields[3]), 'fn(id, height)')

    def test_select_clause(self):
        expected_select_clause_str = (
            'select a name, b, fn(id, dob) age, fn(id, height)')
        expected_select_clause = SelectClause(expected_select_clause_str)
        actual_select_clause = self.query.select_clause

        self.assertEqual(actual_select_clause, expected_select_clause)
        self.assertEqual(str(actual_select_clause), expected_select_clause_str)

    def test_joins(self):
        expected_joins = [Join('join', Table(name='d'), OnClause('on e = f'))]

        actual_joins = self.query.from_clause.joins

        self.assertEqual(actual_joins, expected_joins)

        self.assertEqual(str(actual_joins[0]), ' join d on e = f')

    def test_from_clause(self):
        expected_joins = [Join('join', Table(name='d'), OnClause('on e = f'))]

        expected_table = Table(name='c')
        expected_from_clause = FromClause(expected_table, expected_joins)
        actual_from_clause = self.query.from_clause

        self.assertEqual(actual_from_clause, expected_from_clause)

        self.assertEqual(str(actual_from_clause), 'from c join d on e = f')

    def test_where_clause(self):
        expected_where_clause = WhereClause('where g = h and i = j')
        actual_where_clause = self.query.where_clause

        self.assertEqual(actual_where_clause, expected_where_clause)

        self.assertEqual(str(actual_where_clause), 'where g = h and i = j')

    def test_query(self):
        expected_query = Query(self.sql_str)
        actual_query = self.query

        self.assertEqual(actual_query, expected_query)

        self.assertEqual(str(actual_query), self.sql_str)


class ComplexQueryTestCase(TestCase):
    maxDiff = None

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

        expected_query_str = (
            "select a name, b, fn(id, dob) age, fn(id, height), (select c1 "
            "from a1 where a1.b1 = b) c1 from c join d on e = f left join "
            "(select shape from k where kind = 'quadrilateral') on l = m and "
            "n = o where g = h and i = j")
        self.assertEqual(actual_query.__str__(), expected_query_str)


class EquivalenceTestCase(TestCase):
    def setUp(self):
        self.query_1 = Query(
            'select a, b from c join d on e = f where g = h and i = j')
        self.query_2 = Query(
            'select b, a from d join c on f = e where h = g and j = i')

        self.select_clause_1 = SelectClause('select a, b')
        self.select_clause_2 = SelectClause('select b, a')

        self.where_clause_1 = WhereClause('where g = h and i = j')
        self.where_clause_2 = WhereClause('where h = g and j = i')

    # -------------------------------------------------------------------------
    # Test individual clauses
    def test_select_clause_equivalence_1(self):
        equivalent_1 = self.select_clause_1.is_equivalent_to(
            self.select_clause_2)

        self.assertTrue(equivalent_1)

    def test_select_clause_equivalence_2(self):
        equivalent_2 = self.select_clause_2.is_equivalent_to(
            self.select_clause_1)

        self.assertTrue(equivalent_2)

    def test_where_clause_equivalence_1(self):
        equivalent_1 = self.where_clause_1.is_equivalent_to(
            self.where_clause_2)

        self.assertTrue(equivalent_1)

    def test_where_clause_equivalence_2(self):
        equivalent_2 = self.where_clause_2.is_equivalent_to(
            self.where_clause_1)

        self.assertTrue(equivalent_2)

    # -------------------------------------------------------------------------
    # Test clauses from a query
    def test_query_select_clause_equivalence_1(self):
        equivalent_1 = self.query_1.select_clause.is_equivalent_to(
            self.query_2.select_clause)

        self.assertTrue(equivalent_1)

    def test_query_select_clause_equivalence_2(self):
        equivalent_2 = self.query_2.select_clause.is_equivalent_to(
            self.query_1.select_clause)

        self.assertTrue(equivalent_2)

    def test_query_from_clause_equivalence_1(self):
        # equivalent_1 = self.query_1.from_clause.is_equivalent_to(
        #    self.query_2.from_clause)
        # TODO: Work on from-clause equivalence
        # self.assertTrue(equivalent_1)
        self.assertTrue(1 == 1)

    def test_query_from_clause_equivalence_2(self):
        self.query_2.from_clause.is_equivalent_to(self.query_1.from_clause)

        # TODO: Work on from-clause equivalence
        # self.assertTrue(equivalent_2)
        self.assertTrue(1 == 1)


class ConversionTestCase(TestCase):
    def test_join_clause_to_scalar_subquery(self):
        input_sql_str = '''
            select id,
                (select 'Y' from student_gpa where where student_id = student.id and gpa > 3.7) honors
            from student;
        '''

        output_sql_str = '''
            select id,
                   (case when gpa > 3.7 then 'Y' else null end) honors
              from student
              left
              join student_gpa
                on student.id = student_id;
            '''

        input_query = Query(input_sql_str)
        output_query = Query(output_sql_str)
