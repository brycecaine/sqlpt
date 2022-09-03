from unittest import TestCase

from sqlalchemy.engine import Engine
from sqlpt.sql import DataSet, Expression, SelectClause, Table, ExpressionClause


class DataSetTestCase(TestCase):
    def setUp(self):
        super().setUp()

        self.db_conn_str = 'sqlite:///sqlpt/college.db'

    def test_data_set_create(self):
        dataset = DataSet(self.db_conn_str)

        self.assertEqual(dataset.db_conn_str, self.db_conn_str)

    def test_data_set_db_conn_exists(self):
        dataset = DataSet(self.db_conn_str)

        self.assertEqual(type(dataset.db_conn), Engine)

    def test_data_set_db_conn_none(self):
        with self.assertRaises(TypeError):
            DataSet()

    def test_data_set_rows_unique(self):
        dataset = DataSet(self.db_conn_str)

        with self.assertRaises(Exception):
            dataset.rows_unique(['a'])


class TableTestCase(TestCase):
    def setUp(self):
        super().setUp()

        self.db_conn_str = 'sqlite:///sqlpt/college.db'

    def test_table_create(self):
        table = Table(name='student_section', db_conn_str=self.db_conn_str)

        self.assertEqual(table.name, 'student_section')

    def test_table_rows_unique(self):
        table = Table(name='student_section', db_conn_str=self.db_conn_str)
        field_names = ['student_id', 'term_id', 'section_id']
        actual_uniqueness = table.rows_unique(field_names)
        expected_uniqueness = True

        self.assertEqual(actual_uniqueness, expected_uniqueness)

    def test_table_rows_not_unique(self):
        table = Table(name='student_section', db_conn_str=self.db_conn_str)
        field_names = ['term_id']
        actual_uniqueness = table.rows_unique(field_names)
        expected_uniqueness = False

        self.assertEqual(actual_uniqueness, expected_uniqueness)

    def test_table_count(self):
        table = Table(name='student_section', db_conn_str=self.db_conn_str)
        ct = table.count()
        expected_ct = 4

        self.assertEqual(ct, expected_ct)

    def test_table_get_column_names(self):
        table = Table(name='student_section', db_conn_str=self.db_conn_str)
        column_names = table.get_column_names()
        expected_column_names = ['id', 'student_id', 'term_id', 'section_id']

        self.assertEqual(column_names, expected_column_names)

    def test_table_equivalence(self):
        table_1 = Table(name='student_section', db_conn_str=self.db_conn_str)
        table_2 = Table(name='student_section', db_conn_str=self.db_conn_str)

        self.assertEqual(table_1, table_2)


class SelectClauseTestCase(TestCase):
    def test_select_clause_create(self):
        select_clause = SelectClause('select a, b from term')

        self.assertTrue(select_clause)

    def test_select_clause_field_names(self):
        select_clause = SelectClause('select a, b from term')

        expected_field_names = ['a', 'b']

        self.assertEqual(select_clause.field_names, expected_field_names)

    def test_select_clause_add_field(self):
        select_clause = SelectClause('select a, b from term')

        field_name = 'c'
        select_clause.add_field(field_name)

        expected_field_names = ['a', 'b', 'c']

        self.assertEqual(select_clause.field_names, expected_field_names)

    def test_select_clause_remove_field(self):
        select_clause = SelectClause('select a, b, c from term')

        field_name = 'c'
        select_clause.remove_field(field_name)

        expected_field_names = ['a', 'b']

        self.assertEqual(select_clause.field_names, expected_field_names)

    def test_select_locate_field(self):
        select_clause = SelectClause('select a, b, c from term')

        field_names = ['a', 'b', 'c']

        for i, field_name in enumerate(field_names):
            locations = select_clause.locate_field(field_name)

            expected_locations = [('select_clause', 'fields', i)]

            self.assertEqual(locations, expected_locations)

    def test_select_clause_is_equivalent_to(self):
        select_clause_1 = SelectClause('select a, b')
        select_clause_2 = SelectClause('select b, a')

        self.assertTrue(select_clause_1.is_equivalent_to(select_clause_2))

    def test_select_clause_equivalence(self):
        select_clause_1 = SelectClause('select a, b')
        select_clause_2 = SelectClause('select a, b')

        self.assertEqual(select_clause_1, select_clause_2)


class ExpressionTestCase(TestCase):
    def test_expression_create(self):
        expression = Expression(s_str='a = b')

        comparison = expression.comparisons[0]

        self.assertEqual(comparison.bool_conjunction, '')
        self.assertEqual(comparison.bool_sign, '')
        self.assertEqual(str(comparison), 'a = b')

    def test_expression_and(self):
        expression = Expression(s_str='a = b and c = d')

        comparison_0 = expression.comparisons[0]
        comparison_1 = expression.comparisons[1]

        self.assertEqual(comparison_0.bool_conjunction, '')
        self.assertEqual(comparison_0.bool_sign, '')
        self.assertEqual(str(comparison_0), 'a = b')

        self.assertEqual(comparison_1.bool_conjunction, 'and')
        self.assertEqual(comparison_1.bool_sign, '')
        self.assertEqual(comparison_1.left_term, 'c')
        self.assertEqual(comparison_1.operator, '=')
        self.assertEqual(comparison_1.right_term, 'd')
        self.assertEqual(str(comparison_1), 'and c = d')

    def test_expression_and_not(self):
        expression = Expression(s_str='a = b and not c = d')

        comparison_0 = expression.comparisons[0]
        comparison_1 = expression.comparisons[1]

        self.assertEqual(comparison_0.bool_conjunction, '')
        self.assertEqual(comparison_0.bool_sign, '')
        self.assertEqual(str(comparison_0), 'a = b')

        self.assertEqual(comparison_1.bool_conjunction, 'and')
        self.assertEqual(comparison_1.bool_sign, 'not')
        self.assertEqual(comparison_1.left_term, 'c')
        self.assertEqual(comparison_1.operator, '=')
        self.assertEqual(comparison_1.right_term, 'd')
        self.assertEqual(str(comparison_1), 'and not c = d')

    def test_expression_or(self):
        expression = Expression(s_str='a = b or c = d')

        comparison_0 = expression.comparisons[0]
        comparison_1 = expression.comparisons[1]

        self.assertEqual(comparison_0.bool_conjunction, '')
        self.assertEqual(comparison_0.bool_sign, '')
        self.assertEqual(str(comparison_0), 'a = b')

        self.assertEqual(comparison_1.bool_conjunction, 'or')
        self.assertEqual(comparison_1.bool_sign, '')
        self.assertEqual(comparison_1.left_term, 'c')
        self.assertEqual(comparison_1.operator, '=')
        self.assertEqual(comparison_1.right_term, 'd')
        self.assertEqual(str(comparison_1), 'or c = d')

    def test_expression_or_not(self):
        expression = Expression(s_str='a = b or not c = d')

        comparison_0 = expression.comparisons[0]
        comparison_1 = expression.comparisons[1]

        self.assertEqual(comparison_0.bool_conjunction, '')
        self.assertEqual(comparison_0.bool_sign, '')
        self.assertEqual(str(comparison_0), 'a = b')

        self.assertEqual(comparison_1.bool_conjunction, 'or')
        self.assertEqual(comparison_1.bool_sign, 'not')
        self.assertEqual(comparison_1.left_term, 'c')
        self.assertEqual(comparison_1.operator, '=')
        self.assertEqual(comparison_1.right_term, 'd')
        self.assertEqual(str(comparison_1), 'or not c = d')


class QueryTestCase(TestCase):
    def test_query_rows_unique(self):
        pass

    def test_query_rows_not_unique(self):
        pass


class ExpressionClauseTestCase(TestCase):
    def test_expression_clause_create(self):
        expression = Expression(s_str='a = b')
        expression_clause = ExpressionClause(leading_word='where', expression=expression)

        self.assertTrue(expression_clause)

    def test_expression_clause_parse(self):
        with self.assertRaises(NotImplementedError):
            s_str = 'a = b'
            expression = Expression(s_str=s_str)
            expression_clause = ExpressionClause(leading_word='where', expression=expression)
            expression_clause.parse_expression_clause(s_str)

    def test_expression_clause_is_equivalent_to(self):
        s_str_1 = 'a = b'
        expression_1 = Expression(s_str=s_str_1)
        expression_clause_1 = ExpressionClause(leading_word='where', expression=expression_1)

        s_str_2 = 'b = a'
        expression_2 = Expression(s_str=s_str_2)
        expression_clause_2 = ExpressionClause(leading_word='where', expression=expression_2)

        self.assertTrue(expression_clause_1.is_equivalent_to(expression_clause_2))

    def test_expression_clause_get_expression_clause_parse(self):
        with self.assertRaises(NotImplementedError):
            s_str = 'a = b'
            expression = Expression(s_str=s_str)
            expression_clause = ExpressionClause(leading_word='where', expression=expression)

            expression_clause_token_list = (expression_clause.parse_expression_clause(s_str))
            expression = self.__class__.get_expression_clause_parts(expression_clause_token_list)

            self.assertTrue(expression)
