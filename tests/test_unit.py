""" docstring tbd """

from unittest import TestCase

from sqlpt import service
from sqlpt.sql import (
    Comparison, DeleteClause, DeleteStatement, Field, FromClause,
    Join, OnClause, Query, SelectClause, SetClause, Table, UpdateClause,
    UpdateStatement, WhereClause, parse_fields)


class StringTestCase(TestCase):
    """ docstring tbd """
    def _test(self, sql_str, expected_select_clause, expected_from_clause,
              expected_where_clause):
        """ docstring tbd """

        db_conn_str = 'sqlite:///sqlpt/college.db'
        query = Query(sql_str, db_conn_str)

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
        expected_from_clause = FromClause('from dual', db_conn_str)
        expected_where_clause = WhereClause(s_str="where dummy = 'X'")

        self._test(sql_str, expected_select_clause, expected_from_clause,
                   expected_where_clause)

    def test_query_single_field(self):
        """ docstring tbd """
        sql_str = "select fld_1 from dual where dummy = 'X'"

        db_conn_str = 'sqlite:///sqlpt/college.db'

        expected_select_clause = SelectClause('select fld_1')
        expected_from_clause = FromClause('from dual', db_conn_str)
        expected_where_clause = WhereClause(s_str="where dummy = 'X'")

        self._test(sql_str, expected_select_clause, expected_from_clause,
                   expected_where_clause)

    def test_query_multiple_fields(self):
        """ docstring tbd """
        sql_str = "select fld_1, fld_2 from dual where dummy = 'X'"

        db_conn_str = 'sqlite:///sqlpt/college.db'

        expected_select_clause = SelectClause('select fld_1, fld_2')
        expected_from_clause = FromClause('from dual', db_conn_str)
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
        query = Query(sql_str)

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

        query = Query(sql_str)
        select_subquery = query.select_clause.fields[4].query
        join_subquery = query.from_clause.joins[1].dataset

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

        query = Query(sql_str)

        actual_sql_str = str(query)
        expected_sql_str = service.remove_whitespace_from_str(sql_str)

        self.assertEqual(actual_sql_str, expected_sql_str)

    def test_subquery_in_from_clause(self):
        """ docstring tbd """
        sql_str = '''
            select a
              from (select a from tbl)
            '''

        query = Query(sql_str)

        actual_sql_str = str(query)
        expected_sql_str = service.remove_whitespace_from_str(sql_str)

        self.assertEqual(type(query.from_clause.from_dataset), Query)
        self.assertEqual(actual_sql_str, expected_sql_str)

    def test_sole_select_clause(self):
        """ docstring tbd """
        sql_str = '''
            select id
            '''

        query = Query(sql_str)

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


class QueryTestCase(TestCase):
    """ docstring tbd """
    def setUp(self):
        """ docstring tbd """
        self.sql_str = ('select a name, b, fn(id, dob) age, fn(id, height) '
                        'from c join d on e = f where g = h and i = j')
        db_conn_str = 'sqlite:///sqlpt/college.db'
        self.query = Query(self.sql_str, db_conn_str)

    def test_fields(self):
        """ docstring tbd """
        expected_fields = [
            Field('a name'),
            Field('b'),
            Field('fn(id, dob) age'),
            Field('fn(id, height)'),
        ]
        actual_fields = self.query.select_clause.fields

        self.assertEqual(actual_fields, expected_fields)

        self.assertEqual(str(actual_fields[0]), 'a name')
        self.assertEqual(str(actual_fields[1]), 'b')
        self.assertEqual(str(actual_fields[2]), 'fn(id, dob) age')
        self.assertEqual(str(actual_fields[3]), 'fn(id, height)')

    def test_select_clause(self):
        """ docstring tbd """
        expected_select_clause_str = (
            'select a name, b, fn(id, dob) age, fn(id, height)')
        expected_select_clause = SelectClause(expected_select_clause_str)
        actual_select_clause = self.query.select_clause

        self.assertEqual(actual_select_clause, expected_select_clause)
        self.assertEqual(str(actual_select_clause), expected_select_clause_str)

    def test_joins(self):
        """ docstring tbd """
        db_conn_str = 'sqlite:///sqlpt/college.db'
        join_dict = {
            'kind': 'inner',
            'dataset': Table(name='d', db_conn_str=db_conn_str),
            'on_clause': OnClause(s_str='on e = f')}
        expected_joins = [Join(**join_dict)]

        actual_joins = self.query.from_clause.joins

        self.assertEqual(actual_joins, expected_joins)

        self.assertEqual(str(actual_joins[0]), 'join d on e = f')

    def test_from_clause(self):
        """ docstring tbd """
        db_conn_str = 'sqlite:///sqlpt/college.db'
        join_dict = {
            'kind': 'inner',
            'dataset': Table(name='d', db_conn_str=db_conn_str),
            'on_clause': OnClause(s_str='on e = f')}
        expected_joins = [Join(**join_dict)]

        expected_table = Table(name='c', db_conn_str=db_conn_str)
        expected_from_clause = FromClause(
            from_dataset=expected_table, joins=expected_joins)
        actual_from_clause = self.query.from_clause

        self.assertEqual(actual_from_clause, expected_from_clause)

        self.assertEqual(str(actual_from_clause), 'from c join d on e = f')

    def test_where_clause(self):
        """ docstring tbd """
        expected_where_clause = WhereClause(s_str='where g = h and i = j')
        actual_where_clause = self.query.where_clause

        self.assertEqual(actual_where_clause, expected_where_clause)

        self.assertEqual(str(actual_where_clause), 'where g = h and i = j')

    def test_query(self):
        """ docstring tbd """
        db_conn_str = 'sqlite:///sqlpt/college.db'
        expected_query = Query(self.sql_str, db_conn_str)
        actual_query = self.query

        self.assertEqual(actual_query, expected_query)

        self.assertEqual(str(actual_query), self.sql_str)

    def test_query_without_from_clause(self):
        """ docstring tbd """
        sql_str = 'select 1'
        actual_query = Query(sql_str)

        self.assertEqual(str(actual_query), sql_str)


class ComplexQueryTestCase(TestCase):
    """ docstring tbd """
    maxDiff = None

    def setUp(self):
        """ docstring tbd """
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
        """ docstring tbd """
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
    """ docstring tbd """
    def setUp(self):
        """ docstring tbd """
        self.query_1 = Query(
            'select a, b from c join d on e = f where g = h and i = j')
        self.query_2 = Query(
            'select b, a from d join c on f = e where h = g and j = i')

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

        input_query = Query(input_sql_str)
        output_query = Query(output_sql_str)

        # FUTURE: Eventually assertEqual
        self.assertTrue(input_query)
        self.assertTrue(output_query)


# FUTURE: Make a test case for each class in sql.py
class FieldTestCase(TestCase):
    """ docstring tbd """
    def test_field_str_normal(self):
        """ docstring tbd """
        field = Field('exp a')

        self.assertTrue(field)
        self.assertEqual(field.expression, 'exp')
        self.assertEqual(field.alias, 'a')

    def test_field_list_normal(self):
        """ docstring tbd """
        field = Field('exp a')

        self.assertTrue(field)
        self.assertEqual(field.expression, 'exp')
        self.assertEqual(field.alias, 'a')

    def test_field_str_subquery(self):
        """ docstring tbd """
        field = Field('(select fld from tbl) a')

        self.assertTrue(field)
        self.assertEqual(field.expression, '(select fld from tbl)')
        self.assertEqual(field.alias, 'a')
        self.assertEqual(type(field.query), Query)

    def test_field_list_subquery(self):
        """ docstring tbd """
        field = Field('(select fld from tbl) a')

        self.assertTrue(field)
        self.assertEqual(field.expression, '(select fld from tbl)')
        self.assertEqual(field.alias, 'a')
        self.assertEqual(type(field.query), Query)

    def test_field_no_subquery(self):
        """ docstring tbd """
        field = Field('column_name a')

        self.assertTrue(field)
        self.assertEqual(field.expression, 'column_name')
        self.assertEqual(field.alias, 'a')
        self.assertEqual(field.query, Query())


class ComparisonTestCase(TestCase):
    """ docstring tbd """
    def test_basic(self):
        """ docstring tbd """
        comparison = Comparison('a = b')

        self.assertEqual(comparison.bool_conjunction, '')
        self.assertEqual(comparison.bool_sign, '')
        self.assertEqual(str(comparison), 'a = b')


class UpdateStatementTestCase(TestCase):
    """ docstring tbd """
    def test_update_statement_basic(self):
        """ docstring tbd """
        sql_str = "update student set major = 'BIOL' where id = 4"

        update_clause = UpdateClause('update student')
        set_clause = SetClause(s_str="set major = 'BIOL'")
        where_clause = WhereClause(s_str='where id = 4')

        expected_update_statement = UpdateStatement(
            update_clause=update_clause, set_clause=set_clause,
            where_clause=where_clause)

        self.assertEqual(sql_str, str(expected_update_statement))

    def test_update_statement_count(self):
        """ docstring tbd """
        sql_str = "update student set major = 'BIOL' where id = 4"

        db_conn_str = 'sqlite:///sqlpt/college.db'
        update_statement = UpdateStatement(sql_str, db_conn_str)

        actual_expected_row_count = update_statement.count()
        expected_expected_row_count = 1

        self.assertEqual(actual_expected_row_count,
                         expected_expected_row_count)


class DeleteStatementTestCase(TestCase):
    """ docstring tbd """
    def test_delete_statement_basic(self):
        """ docstring tbd """
        sql_str = "delete from student where id = 4"

        delete_clause = DeleteClause()
        from_clause = FromClause('from student')
        where_clause = WhereClause(s_str='where id = 4')

        expected_delete_statement = DeleteStatement(
            delete_clause=delete_clause, from_clause=from_clause,
            where_clause=where_clause)

        self.assertEqual(sql_str, str(expected_delete_statement))

    def test_delete_statement_count(self):
        """ docstring tbd """
        sql_str = "delete from student where id = 4"

        db_conn_str = 'sqlite:///sqlpt/college.db'
        delete_statement = DeleteStatement(sql_str, db_conn_str)

        actual_expected_row_count = delete_statement.count()
        expected_expected_row_count = 1

        self.assertEqual(actual_expected_row_count,
                         expected_expected_row_count)


class ServiceTestCase(TestCase):
    """ docstring tbd """
    def test_remove_whitespace_from_strings(self):
        """ docstring tbd """
        input_item_list = ['test', ' ', 'list', ' ', 'of', ' ', 'strings']
        actual_item_list = service.remove_whitespace(input_item_list)
        expected_item_list = ['test', 'list', 'of', 'strings']

        self.assertEqual(actual_item_list, expected_item_list)
