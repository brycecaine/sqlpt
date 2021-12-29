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

        query = Query(sql_str)

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

        query = Query(sql_str)

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

        query = Query(sql_str)

        actual_counts = query.counts()

        expected_counts = {
            'query': 4,
            'student': 4,
            'student_section': 4
        }

        self.assertEqual(actual_counts, expected_counts)

    def test_rows_unique(self):
        """ docstring tbd """
        table = Table('student_section')
        field_names = ['student_id', 'term_id', 'section_id']
        actual_uniqueness = table.rows_unique(field_names)
        expected_uniqueness = True

        self.assertEqual(actual_uniqueness, expected_uniqueness)

    def test_rows_not_unique(self):
        """ docstring tbd """
        table = Table('student_section')
        field_names = ['term_id']
        actual_uniqueness = table.rows_unique(field_names)
        expected_uniqueness = False

        self.assertEqual(actual_uniqueness, expected_uniqueness)


class ModifyingTestCase(TestCase):
    """ docstring tbd """
    def test_left_join_to_select_scalar_subquery(self):
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

        query = Query(sql_str_original)

        actual_scalarized_query = query.scalarize()

        expected_scalarized_query = Query(sql_str_scalarized)

        self.assertEqual(actual_scalarized_query, expected_scalarized_query)

    def test_ignore_dangling_parameters(self):
        """ docstring tbd """
        sql_str_scalarized = '''
            select subject,
                   course_number,
                   (select name from term where section.term_id = term.id) name
              from section
        '''

        query = Query(sql_str_scalarized)

        self.assertEqual(
            query.select_clause.fields[2].query.crop().run().count(), 2)

    def test_query_is_leaf(self):
        """ docstring tbd """
        sql_str_scalarized = '''
            select subject,
                   course_number,
                   (select name from term where section.term_id = term.id) name
              from section
        '''

        query = Query(sql_str_scalarized)

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

        query = Query(sql_str_scalarized)

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

        query = Query(sql_str_scalarized)

        actual_ct = (query.select_clause.fields[2].query
                     .parameterize().run(term_id=2).count())
        expected_ct = 1

        self.assertEqual(actual_ct, expected_ct)
