""" docstring tbd """

from unittest import TestCase

from sqlpt.sql import Query, Table


class ProbingTestCase(TestCase):
    """ docstring tbd """
    def test_query_counts_basic(self):
        """ docstring tbd """
        sql_str = '''
            select *
              from student
        '''

        db_conn_str = 'sqlite:///sqlpt/college.db'
        query = Query(sql_str, db_conn_str)

        actual_counts = query.counts()

        expected_counts = {
            'query': 4,
            'student': 4
        }

        self.assertEqual(actual_counts, expected_counts)

    def test_query_counts_where(self):
        """ docstring tbd """
        sql_str = '''
            select *
              from student
             where id <= 2
        '''

        db_conn_str = 'sqlite:///sqlpt/college.db'
        query = Query(sql_str, db_conn_str)

        actual_counts = query.counts()

        expected_counts = {
            'query': 2,
            'student': 4
        }

        self.assertEqual(actual_counts, expected_counts)

    def test_query_counts_joins(self):
        """ docstring tbd """
        sql_str = '''
            select *
              from student
              join student_section
                on student.id = student_section.student_id
        '''

        db_conn_str = 'sqlite:///sqlpt/college.db'
        query = Query(sql_str, db_conn_str)

        actual_counts = query.counts()

        expected_counts = {
            'query': 4,
            'student': 4,
            'student_section': 4
        }

        self.assertEqual(actual_counts, expected_counts)


class ModifyingTestCase(TestCase):
    """ docstring tbd """
    # TODO: Unskip this when fixed (the term Table wasn't getting the db_conn_str)
    def skip_test_left_join_to_select_scalar_subquery(self):
        """ docstring tbd """
        sql_str_original = '''
            select subject,
                   course_number,
                   name
              from section
              left
              join term
                on section.term_id = term.id
        '''

        sql_str_scalarized = '''
            select subject,
                   course_number,
                   (select name from term where section.term_id = term.id) name
              from section
        '''

        db_conn_str = 'sqlite:///sqlpt/college.db'
        query = Query(sql_str_original, db_conn_str)

        actual_scalarized_query = query.scalarize()
        actual_scalarized_query.db_conn_str = db_conn_str

        print('baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaad')
        expected_scalarized_query = Query(sql_str_scalarized, db_conn_str)
        print('baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaad')

        print(actual_scalarized_query.select_clause.fields[2].query.from_clause.__dict__)
        print('ddddddddddddddddd')
        print(expected_scalarized_query.select_clause.fields[2].query.from_clause.__dict__)

        self.assertEqual(actual_scalarized_query, expected_scalarized_query)

    def test_ignore_dangling_parameters(self):
        """ docstring tbd """
        sql_str_scalarized = '''
            select subject,
                   course_number,
                   (select name from term where section.term_id = term.id) name
              from section
        '''

        db_conn_str = 'sqlite:///sqlpt/college.db'
        query = Query(sql_str_scalarized, db_conn_str=db_conn_str)

        subquery = query.select_clause.fields[2].query
        subquery.db_conn_str = db_conn_str

        actual_count = subquery.crop().count()
        expected_count = 2

        self.assertEqual(actual_count, expected_count)

    def test_query_is_leaf(self):
        """ docstring tbd """
        sql_str_scalarized = '''
            select subject,
                   course_number,
                   (select name from term where section.term_id = term.id) name
              from section
        '''

        db_conn_str = 'sqlite:///sqlpt/college.db'
        query = Query(sql_str_scalarized, db_conn_str)

        self.assertFalse(query.is_leaf())
        self.assertTrue(query.select_clause.fields[2].query.is_leaf())

    def test_parameterize(self):
        """ docstring tbd """
        sql_str_scalarized = '''
            select subject,
                   course_number,
                   (select name from term where section.term_id = term.id) name
              from section
        '''

        db_conn_str = 'sqlite:///sqlpt/college.db'
        query = Query(sql_str_scalarized, db_conn_str)

        query.select_clause.fields[2].query.db_conn_str = db_conn_str

        actual_sql = str(query.select_clause.fields[2].query.parameterize())
        expected_sql = 'select name from term where term.id = :term_id'

        self.assertEqual(actual_sql, expected_sql)

    def test_run_parameterized_query(self):
        """ docstring tbd """
        sql_str_scalarized = '''
            select subject,
                   course_number,
                   (select name from term where section.term_id = term.id) name
              from section
        '''

        db_conn_str = 'sqlite:///sqlpt/college.db'
        query = Query(sql_str_scalarized, db_conn_str)

        query.select_clause.fields[2].query.db_conn_str = db_conn_str

        actual_ct = (query.select_clause.fields[2].query
                     .parameterize().run(term_id=2).count())
        expected_ct = 1

        self.assertEqual(actual_ct, expected_ct)
