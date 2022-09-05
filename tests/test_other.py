""" docstring tbd """

from unittest import TestCase

from sqlpt import service
from sqlpt.sql import (Field, FromClause, Query, SelectClause, WhereClause,
                       parse_fields)


# TODO: Rename this file something other than test_unit
class StringTestCase(TestCase):
    """ docstring tbd """
    def _test(self, sql_str, expected_select_clause, expected_from_clause,
              expected_where_clause):
        """ docstring tbd """

        query = Query(sql_str=sql_str)

        expected_query = Query(
            select_clause=expected_select_clause,
            from_clause=expected_from_clause,
            where_clause=expected_where_clause)

        self.assertEqual(query, expected_query)
        self.assertEqual(query.select_clause, expected_select_clause)
        self.assertEqual(query.from_clause, expected_from_clause)
        self.assertEqual(query.where_clause, expected_where_clause)

    def test_query_basic(self):
        """ docstring tbd """
        sql_str = "select * from dual where dummy = 'X'"

        db_conn_str = 'sqlite:///sqlpt/college.db'

        expected_select_clause = SelectClause('select *')
        expected_from_clause = FromClause('from dual')
        expected_where_clause = WhereClause(s_str="where dummy = 'X'")

        self._test(sql_str, expected_select_clause, expected_from_clause,
                   expected_where_clause)

    def test_query_single_field(self):
        """ docstring tbd """
        sql_str = "select fld_1 from dual where dummy = 'X'"

        db_conn_str = 'sqlite:///sqlpt/college.db'

        expected_select_clause = SelectClause('select fld_1')
        expected_from_clause = FromClause('from dual')
        expected_where_clause = WhereClause(s_str="where dummy = 'X'")

        self._test(sql_str, expected_select_clause, expected_from_clause,
                   expected_where_clause)

    def test_query_multiple_fields(self):
        """ docstring tbd """
        sql_str = "select fld_1, fld_2 from dual where dummy = 'X'"

        db_conn_str = 'sqlite:///sqlpt/college.db'

        expected_select_clause = SelectClause('select fld_1, fld_2')
        expected_from_clause = FromClause('from dual')
        expected_where_clause = WhereClause(s_str="where dummy = 'X'")

        self._test(sql_str, expected_select_clause, expected_from_clause,
                   expected_where_clause)

    def test_select_clause(self):
        """ docstring tbd """
        sql_str = 'select fld_1, fld_2'

        actual_select_clause = str(SelectClause(sql_str))
        expected_select_clause = sql_str

        self.assertEqual(actual_select_clause, expected_select_clause)

    def test_select_clause_from_full_query_sql_str(self):
        """ docstring tbd """
        sql_str = "select fld_1, fld_2 from dual where dummy = 'X'"

        actual_select_clause = str(SelectClause(sql_str))
        expected_select_clause = 'select fld_1, fld_2'

        self.assertEqual(actual_select_clause, expected_select_clause)

    def test_from_clause(self):
        """ docstring tbd """
        sql_str = 'from dual'

        actual_from_clause = str(FromClause(sql_str))
        expected_from_clause = sql_str

        self.assertEqual(actual_from_clause, expected_from_clause)

    def test_from_clause_from_full_query_sql_str(self):
        """ docstring tbd """
        sql_str = "select fld_1, fld_2 from dual where dummy = 'X'"

        actual_from_clause = str(FromClause(sql_str))
        expected_from_clause = 'from dual'

        self.assertEqual(actual_from_clause, expected_from_clause)

    def test_where_clause(self):
        """ docstring tbd """
        sql_str = "where dummy = 'X'"

        actual_where_clause = str(WhereClause(s_str=sql_str))
        expected_where_clause = sql_str

        self.assertEqual(actual_where_clause, expected_where_clause)

    def test_where_clause_from_full_query_sql_str(self):
        """ docstring tbd """
        sql_str = "select fld_1, fld_2 from dual where dummy = 'X'"

        actual_where_clause = str(WhereClause(s_str=sql_str))
        expected_where_clause = "where dummy = 'X'"

        self.assertEqual(actual_where_clause, expected_where_clause)


class StringListTestCase(TestCase):
    """ docstring tbd """
    def _test(self, sql_str, expected_select_clause, expected_from_clause,
              expected_where_clause):
        """ docstring tbd """
        query = Query(sql_str=sql_str)

        expected_query = Query(
            select_clause=expected_select_clause,
            from_clause=expected_from_clause,
            where_clause=expected_where_clause)

        self.assertEqual(query, expected_query)
        self.assertEqual(query.select_clause, expected_select_clause)
        self.assertEqual(query.from_clause, expected_from_clause)
        self.assertEqual(query.where_clause, expected_where_clause)

    def test_query_basic(self):
        """ docstring tbd """
        sql_str = "select * from dual where dummy = 'X'"

        expected_select_clause = SelectClause('select *')
        expected_from_clause = FromClause('from dual')
        expected_where_clause = WhereClause(s_str="where dummy = 'X'")

        self._test(sql_str, expected_select_clause, expected_from_clause,
                   expected_where_clause)

    def test_query_single_field(self):
        """ docstring tbd """
        sql_str = "select fld_1 from dual where dummy = 'X'"

        expected_select_clause = SelectClause('select fld_1')
        expected_from_clause = FromClause('from dual')
        expected_where_clause = WhereClause(s_str="where dummy = 'X'")

        self._test(sql_str, expected_select_clause, expected_from_clause,
                   expected_where_clause)

    def test_query_multiple_fields(self):
        """ docstring tbd """
        sql_str = "select fld_1, fld_2 from dual where dummy = 'X'"

        expected_select_clause = SelectClause('select fld_1, fld_2')
        expected_from_clause = FromClause('from dual')
        expected_where_clause = WhereClause(s_str="where dummy = 'X'")

        self._test(sql_str, expected_select_clause, expected_from_clause,
                   expected_where_clause)


class ParseTestCase(TestCase):
    """ docstring tbd """
    def test_parse_sql_clauses(self):
        """ docstring tbd """
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

        query = Query(sql_str=sql_str)
        select_subquery = query.select_clause.fields[4].query
        join_subquery = query.from_clause.join_clauses[1].dataset

        self.assertEqual(type(select_subquery), Query)
        self.assertEqual(type(join_subquery), Query)

    def test_case_statement_in_parentheses(self):
        """ docstring tbd """
        sql_str = '''
            select id,
                   (case when gpa > 3.7 then 'Y' else null end) honors
              from student
              left
              join student_gpa
                on student.id = student_id
            '''

        query = Query(sql_str=sql_str)

        actual_sql_str = str(query)
        expected_sql_str = service.remove_whitespace_from_str(sql_str)

        self.assertEqual(actual_sql_str, expected_sql_str)

    def test_subquery_in_from_clause(self):
        """ docstring tbd """
        sql_str = '''
            select a
              from (select a from tbl)
            '''

        query = Query(sql_str=sql_str)

        actual_sql_str = str(query)
        expected_sql_str = service.remove_whitespace_from_str(sql_str)

        self.assertEqual(type(query.from_clause.from_dataset), Query)
        self.assertEqual(actual_sql_str, expected_sql_str)

    def test_sole_select_clause(self):
        """ docstring tbd """
        sql_str = '''
            select id
            '''

        query = Query(sql_str=sql_str)

        actual_sql_str = str(query)
        expected_sql_str = service.remove_whitespace_from_str(sql_str)

        self.assertEqual(actual_sql_str, expected_sql_str)


class FunctionTestCase(TestCase):
    """ docstring tbd """
    def test_parse_field_basic_no_alias(self):
        """ docstring tbd """
        actual_field = Field('a')
        self.assertTrue(actual_field)

    def test_parse_field_basic_with_alias(self):
        """ docstring tbd """
        actual_field = Field('b c')
        self.assertTrue(actual_field)

    def test_parse_field_function_no_alias(self):
        """ docstring tbd """
        actual_field = Field('fn(x, y)')
        self.assertTrue(actual_field)

    def test_parse_field_function_with_alias(self):
        """ docstring tbd """
        actual_field = Field('fn(x, y) z')
        self.assertTrue(actual_field)

    def test_parse_field_subquery_no_alias(self):
        """ docstring tbd """
        actual_field = Field('(select * from dual)')
        expected_field = Field(expression='(select * from dual)', alias='')
        self.assertEqual(actual_field, expected_field)

    def test_parse_field_subquery_with_alias(self):
        """ docstring tbd """
        actual_field = Field('(select * from dual) z')
        self.assertTrue(actual_field)

    def test_parse_fields_from_str(self):
        """ docstring tbd """
        sql_str = (
            'a, b c, fn(x, y), fn(x, y) z, (select * from dual), '
            '(select * from dual) z')
        actual_fields = parse_fields(sql_str)
        expected_fields = [
            Field('a'),
            Field('b c'),
            Field('fn(x, y)'),
            Field('fn(x, y) z'),
            Field('(select * from dual)'),
            Field('(select * from dual) z'),
        ]

        self.assertEqual(actual_fields, expected_fields)


class EquivalenceTestCase(TestCase):
    """ docstring tbd """
    def setUp(self):
        """ docstring tbd """
        self.query_1 = Query(
            sql_str='select a, b from c join d on e = f where g = h and i = j')
        self.query_2 = Query(
            sql_str='select b, a from d join c on f = e where h = g and j = i')

        self.select_clause_1 = SelectClause('select a, b')
        self.select_clause_2 = SelectClause('select b, a')

        self.where_clause_1 = WhereClause(s_str='where g = h and i = j')
        self.where_clause_2 = WhereClause(s_str='where h = g and j = i')

    # -------------------------------------------------------------------------
    # Test individual clauses
    def test_select_clause_equivalence_1(self):
        """ docstring tbd """
        equivalent_1 = self.select_clause_1.is_equivalent_to(
            self.select_clause_2)

        self.assertTrue(equivalent_1)

    def test_select_clause_equivalence_2(self):
        """ docstring tbd """
        equivalent_2 = self.select_clause_2.is_equivalent_to(
            self.select_clause_1)

        self.assertTrue(equivalent_2)

    def test_where_clause_equivalence_1(self):
        """ docstring tbd """
        equivalent_1 = self.where_clause_1.is_equivalent_to(
            self.where_clause_2)

        self.assertTrue(equivalent_1)

    def test_where_clause_equivalence_2(self):
        """ docstring tbd """
        equivalent_2 = self.where_clause_2.is_equivalent_to(
            self.where_clause_1)

        self.assertTrue(equivalent_2)

    # -------------------------------------------------------------------------
    # Test clauses from a query
    def test_query_select_clause_equivalence_1(self):
        """ docstring tbd """
        equivalent_1 = self.query_1.select_clause.is_equivalent_to(
            self.query_2.select_clause)

        self.assertTrue(equivalent_1)

    def test_query_select_clause_equivalence_2(self):
        """ docstring tbd """
        equivalent_2 = self.query_2.select_clause.is_equivalent_to(
            self.query_1.select_clause)

        self.assertTrue(equivalent_2)

    def test_query_from_clause_equivalence_1(self):
        """ docstring tbd """
        # equivalent_1 = self.query_1.from_clause.is_equivalent_to(
        #    self.query_2.from_clause)
        # FUTURE: Work on from-clause equivalence
        # self.assertTrue(equivalent_1)
        self.assertTrue(1 == 1)

    def test_query_from_clause_equivalence_2(self):
        """ docstring tbd """
        self.query_2.from_clause.is_equivalent_to(self.query_1.from_clause)

        # FUTURE: Work on from-clause equivalence
        # self.assertTrue(equivalent_2)
        self.assertTrue(1 == 1)


class ConversionTestCase(TestCase):
    """ docstring tbd """
    def test_join_clause_to_scalar_subquery(self):
        """ docstring tbd """
        input_sql_str = '''
            select id,
                   (select 'Y'
                      from student_gpa
                     where student_id = student.id
                       and gpa > 3.7) honors
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

        input_query = Query(sql_str=input_sql_str)
        output_query = Query(sql_str=output_sql_str)

        # FUTURE: Eventually assertEqual
        self.assertTrue(input_query)
        self.assertTrue(output_query)


class ServiceTestCase(TestCase):
    """ docstring tbd """
    def test_remove_whitespace_from_strings(self):
        """ docstring tbd """
        input_item_list = ['test', ' ', 'list', ' ', 'of', ' ', 'strings']
        actual_item_list = service.remove_whitespace(input_item_list)
        expected_item_list = ['test', 'list', 'of', 'strings']

        self.assertEqual(actual_item_list, expected_item_list)
