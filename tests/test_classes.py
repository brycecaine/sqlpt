from unittest import TestCase

from sqlalchemy.engine import Engine
from sqlpt.sql import DataSet, Expression, SelectClause, Table, OnClause, ExpressionClause, WhereClause, HavingClause, SetClause, QueryResult, Join


class QueryResultTestCase(TestCase):
    def test_query_result_create(self):
        query_result = QueryResult()
        query_result.append('a')

        self.assertEqual(query_result.count(), 1)


class DataSetTestCase(TestCase):
    def setUp(self):
        super().setUp()

        self.db_conn_str = 'sqlite:///sqlpt/college.db'

    def test_dataset_create(self):
        dataset = DataSet(self.db_conn_str)

        self.assertEqual(dataset.db_conn_str, self.db_conn_str)

    def test_dataset_db_conn_exists(self):
        dataset = DataSet(self.db_conn_str)

        self.assertEqual(type(dataset.db_conn), Engine)

    def test_dataset_db_conn_none(self):
        with self.assertRaises(TypeError):
            DataSet()

    def test_dataset_rows_unique(self):
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
            expression = ExpressionClause.get_expression_clause_parts(expression_clause_token_list)

            self.assertTrue(expression)


class OnClauseTestCase(TestCase):
    def test_on_clause_create(self):
        expression = Expression(s_str='a = b')
        on_clause = OnClause(expression=expression)

        self.assertEqual(on_clause.leading_word, 'on')

    def test_on_clause_parse(self):
        s_str = 'a = b'
        expression = Expression(s_str=s_str)
        on_clause = OnClause(expression=expression)
        on_clause.parse_expression_clause(s_str)

    def test_on_clause_is_equivalent_to(self):
        s_str_1 = 'a = b'
        expression_1 = Expression(s_str=s_str_1)
        on_clause_1 = OnClause(expression=expression_1)

        s_str_2 = 'b = a'
        expression_2 = Expression(s_str=s_str_2)
        on_clause_2 = OnClause(expression=expression_2)

        self.assertTrue(on_clause_1.is_equivalent_to(on_clause_2))

    def test_on_clause_get_expression_clause_parse(self):
        s_str = 'a = b'
        expression = Expression(s_str=s_str)
        on_clause = OnClause(expression=expression)

        on_clause_token_list = (on_clause.parse_expression_clause(s_str))
        expression = ExpressionClause.get_expression_clause_parts(on_clause_token_list)

        self.assertTrue(expression)


class JoinTestCase(TestCase):
    def test_join_create(self):
        on_clause = OnClause('b = c')
        join = Join(kind='left', dataset='a', on_clause=on_clause)

        self.assertTrue(join)

    def test_join_simple_kind(self):
        on_clause = OnClause('b = c')

        join = Join(kind='inner', dataset='a', on_clause=on_clause)
        self.assertEqual(join.simple_kind, 'join')

        join = Join(kind='left', dataset='a', on_clause=on_clause)
        self.assertEqual(join.simple_kind, 'left join')

        join = Join(kind='right', dataset='a', on_clause=on_clause)
        self.assertEqual(join.simple_kind, 'right join')

    def test_join_kind(self):
        on_clause = OnClause('b = c')

        join = Join(kind='inner', dataset='a', on_clause=on_clause)
        self.assertEqual(str(join).split(' ')[0], 'join')

        join = Join(kind='left', dataset='a', on_clause=on_clause)
        self.assertEqual(str(join).split(' ')[0], 'left')

        join = Join(kind='right', dataset='a', on_clause=on_clause)
        self.assertEqual(str(join).split(' ')[0], 'right')

    def test_join_is_equivalent_to(self):
        # TODO: Put this db defintion in a central place
        db_conn_str = 'sqlite:///sqlpt/college.db'
        # TODO: Allow all classes to accept a single s_str argument or keyword args
        dataset = Table(name='a', db_conn_str=db_conn_str)

        on_clause_1 = OnClause('b = c')
        join_1 = Join(kind='left', dataset=dataset, on_clause=on_clause_1)

        on_clause_2 = OnClause('c = b')
        join_2 = Join(kind='left', dataset=dataset, on_clause=on_clause_2)

        self.assertTrue(join_1.is_equivalent_to(join_2))


class FromClauseTestCase(TestCase):
    def test_join_clause_create(self):
        # TODO: Come back to this after testing all expression clause types
        pass


class ComparisonTestCase(TestCase):
    def test_join_clause_create(self):
        # TODO: Come back to this after testing all expression clause types
        pass


class WhereClauseTestCase(TestCase):
    def test_where_clause_create(self):
        expression = Expression(s_str='a = b')
        where_clause = WhereClause(expression=expression)

        self.assertTrue(where_clause)

    def test_where_clause_parse(self):
        s_str = 'a = b'
        expression = Expression(s_str=s_str)
        where_clause = WhereClause(expression=expression)
        where_clause.parse_expression_clause(s_str)

    def test_where_clause_is_equivalent_to(self):
        s_str_1 = 'a = b'
        expression_1 = Expression(s_str=s_str_1)
        where_clause_1 = WhereClause(expression=expression_1)

        s_str_2 = 'b = a'
        expression_2 = Expression(s_str=s_str_2)
        where_clause_2 = WhereClause(expression=expression_2)

        self.assertTrue(where_clause_1.is_equivalent_to(where_clause_2))

    def test_where_clause_get_expression_clause_parse(self):
        s_str = 'a = b'
        expression = Expression(s_str=s_str)
        where_clause = WhereClause(expression=expression)

        where_clause_token_list = (where_clause.parse_expression_clause(s_str))
        expression = ExpressionClause.get_expression_clause_parts(where_clause_token_list)

        self.assertTrue(expression)

    def test_where_locate_field(self):
        where_clause = WhereClause(s_str='where a = b and c = d')

        location_a = where_clause.locate_field('a')
        expected_locations_a = [('where_clause', 'expression', 'comparisons', 0, 'left_term')]

        location_b = where_clause.locate_field('b')
        expected_locations_b = [('where_clause', 'expression', 'comparisons', 0, 'right_term')]

        location_c = where_clause.locate_field('c')
        expected_locations_c = [('where_clause', 'expression', 'comparisons', 1, 'left_term')]

        location_d = where_clause.locate_field('d')
        expected_locations_d = [('where_clause', 'expression', 'comparisons', 1, 'right_term')]

        self.assertEqual(location_a, expected_locations_a)
        self.assertEqual(location_b, expected_locations_b)
        self.assertEqual(location_c, expected_locations_c)
        self.assertEqual(location_d, expected_locations_d)


class GroupByClauseTestCase(TestCase):
    def test_join_clause_create(self):
        # TODO: Come back to this after testing all expression clause types
        pass


class HavingClauseTestCase(TestCase):
    def test_having_clause_create(self):
        expression = Expression(s_str='a = b')
        having_clause = HavingClause(expression=expression)

        self.assertTrue(having_clause)

    def test_having_clause_parse(self):
        s_str = 'a = b'
        expression = Expression(s_str=s_str)
        having_clause = HavingClause(expression=expression)
        having_clause.parse_expression_clause(s_str)

    def test_having_clause_is_equivalent_to(self):
        s_str_1 = 'a = b'
        expression_1 = Expression(s_str=s_str_1)
        having_clause_1 = HavingClause(expression=expression_1)

        s_str_2 = 'b = a'
        expression_2 = Expression(s_str=s_str_2)
        having_clause_2 = HavingClause(expression=expression_2)

        self.assertTrue(having_clause_1.is_equivalent_to(having_clause_2))

    def test_having_clause_get_expression_clause_parse(self):
        s_str = 'a = b'
        expression = Expression(s_str=s_str)
        having_clause = HavingClause(expression=expression)

        having_clause_token_list = (having_clause.parse_expression_clause(s_str))
        expression = ExpressionClause.get_expression_clause_parts(having_clause_token_list)

        self.assertTrue(expression)


class QueryTestCase(TestCase):
    def test_query_rows_unique(self):
        pass

    def test_query_rows_not_unique(self):
        pass


# Field
# UpdateClause
# UpdateStatement
# DeleteClause
# DeleteStatement
# get_dataset
# parse_select_clause
# parse_field
# parse_fields
# parse_fields_from_token_list

# TODO: Convert sql_str to s_str where it makes sense


class SetClauseTestCase(TestCase):
    def test_set_clause_create(self):
        expression = Expression(s_str='a = b')
        set_clause = SetClause(expression=expression)

        self.assertTrue(set_clause)

    def test_set_clause_parse(self):
        s_str = 'a = b'
        expression = Expression(s_str=s_str)
        set_clause = SetClause(expression=expression)
        set_clause.parse_expression_clause(s_str)

    def test_set_clause_is_equivalent_to(self):
        s_str_1 = 'a = b'
        expression_1 = Expression(s_str=s_str_1)
        set_clause_1 = SetClause(expression=expression_1)

        s_str_2 = 'b = a'
        expression_2 = Expression(s_str=s_str_2)
        set_clause_2 = SetClause(expression=expression_2)

        self.assertTrue(set_clause_1.is_equivalent_to(set_clause_2))

    def test_set_clause_get_expression_clause_parse(self):
        s_str = 'a = b'
        expression = Expression(s_str=s_str)
        set_clause = SetClause(expression=expression)

        set_clause_token_list = (set_clause.parse_expression_clause(s_str))
        expression = ExpressionClause.get_expression_clause_parts(set_clause_token_list)

        self.assertTrue(expression)
