from unittest import TestCase

from sqlpt.sql import LogicUnit, Query, RecordSet


class FunctionalTestCase(TestCase):
    def setUp(self):
        # TODO: Add "from dual" to support Oracle
        value_sql_str = '''
            select coalesce((select 1
                               from student_section
                              where student_id = :student_id), 0)
            '''

        value_query = Query(value_sql_str)

        self.logic_unit = LogicUnit(name='Registered Student',
                                    query=value_query,
                                    db_source='college.db')

    def test_logic_unit(self):
        actual_result = self.logic_unit.get_value(student_id=1)
        expected_result = 1

        self.assertEqual(actual_result, expected_result)

    def test_record_set_with_logic_unit(self):
        record_set_sql_str = '''
            select *
              from student
            '''

        record_set_query = Query(record_set_sql_str)

        record_set = RecordSet(name='Students',
                               query=record_set_query,
                               db_source='college.db')

        record_set.filter_by_logic_unit(self.logic_unit, '=', 1)

        actual_result = record_set.get_data(student_id='student.id')
        expected_result = [
            (1, 1, '1', 1, 'MATH'),
            (2, 1, '2', 1, 'MATH'),
            (3, 2, '1', 1, 'ENGL'),
            (4, 2, '2', 1, 'ENGL'),
        ]

        self.assertEqual(actual_result, expected_result)
