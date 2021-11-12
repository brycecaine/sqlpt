""" docstring tbd """

from unittest import TestCase

from sqlpt.sql import Query


class DatabaseTestCase(TestCase):
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


class SqlShapingTestCase(TestCase):
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
            query.select_clause.fields[2].query.count(), 2)
