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
