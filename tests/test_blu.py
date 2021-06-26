from unittest import TestCase
from sqlpt.sql import LogicUnit, Query, RecordSet
from sqlpt.service import remove_whitespace_from_str


class FunctionalTestCase(TestCase):
    def setUp(self):
        # TODO: Add "from dual" to support Oracle
        self.value_sql_str = '''
            select coalesce((select 1
                               from student_section
                              where student_id = :student_id), 0)
            '''

        self.record_set_sql_str = '''
            select *
              from student
            '''

    def test_logic_unit(self):
        value_sql_str = remove_whitespace_from_str(self.value_sql_str)
        self.value_query = Query(value_sql_str)

        logic_unit = LogicUnit(name='Registered Student',
                               query=self.value_query,
                               db_source='college.db')

        actual_result = logic_unit.get_value(student_id=1)
        expected_result = 1

        self.assertEqual(actual_result, expected_result)

    def test_record_set_with_logic_unit(self):
        record_set_sql_str = remove_whitespace_from_str(
            self.record_set_sql_str)

        record_set_query = Query(record_set_sql_str)
        subquery_str = (
            ' '.join(self.value_sql_str.strip().replace('\n', '').split()))
        record_set_query.where_clause.add_comparison(f'({subquery_str}) = 1')
        record_set = RecordSet(name='Registered Students',
                               query=record_set_query,
                               db_source='college.db')

        actual_result = record_set.get_data(student_id='student.id')
        expected_result = [
            (1, 1, '1', 1, 'MATH'),
            (2, 1, '2', 1, 'MATH'),
            (3, 2, '1', 1, 'ENGL'),
            (4, 2, '2', 1, 'ENGL'),
        ]

        self.assertEqual(actual_result, expected_result)
