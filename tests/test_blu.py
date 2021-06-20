from unittest import TestCase
from sqlpt.sql import LogicUnit, Query, remove_whitespace_from_str


class FunctionalTestCase(TestCase):
    def setUp(self):
        # TODO: Add "from dual" to support Oracle
        self.value_sql_str = '''
            select coalesce((select 1
                               from student_section
                              where student_id = :student_id), 0)
            '''

        self.population_sql_str = '''
            select *
              from student
             where (select coalesce((select 1
                                       from student_section
                                      where student_id = student.id), 0)) = 1
            '''

        value_sql_str = remove_whitespace_from_str(
            self.value_sql_str)

        population_sql_str = remove_whitespace_from_str(
            self.population_sql_str)

        self.value_query = Query(value_sql_str)
        self.population_query = Query(population_sql_str)

    def test_logic_unit(self):
        lu = LogicUnit(name='Registered Student',
                       query=self.value_query,
                       db_source='college.db')
        
        actual_result = lu.get_value(student_id=1)
        expected_result = 1

        self.assertEqual(actual_result, expected_result)

    def test_population_of_logic_unit(self):
        lu = LogicUnit(name='Registered Student',
                       query=self.population_query,
                       db_source='college.db')

        actual_result = lu.get_population()
        expected_result = [
            (1, 1, '1', 1, 'MATH'),
            (2, 1, '2', 1, 'MATH'),
            (3, 2, '1', 1, 'ENGL'),
            (4, 2, '2', 1, 'ENGL'),
        ]

        self.assertEqual(actual_result, expected_result)
