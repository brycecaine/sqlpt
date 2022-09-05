from unittest import TestCase

from sqlalchemy.engine import Engine
from sqlpt.sql import (Comparison, DataSet, DeleteClause, DeleteStatement,
                       Expression, ExpressionClause, Field, FromClause,
                       GroupByClause, HavingClause, Join, OnClause, Query,
                       QueryResult, SelectClause, SetClause, Table,
                       UpdateClause, UpdateStatement, WhereClause)

DB_CONN_STR = 'sqlite:///sqlpt/college.db'


class QueryResultTestCase(TestCase):
    def test_query_result_create(self):
        query_result = QueryResult()
        query_result.append('a')

        self.assertEqual(query_result.count(), 1)


class DataSetTestCase(TestCase):
    def test_dataset_create(self):
        dataset = DataSet()

        self.assertTrue(dataset)


class TableTestCase(TestCase):
    def test_table_create(self):
        table = Table(name='student_section', db_conn_str=DB_CONN_STR)

        self.assertEqual(table.name, 'student_section')

    def test_table_db_conn_exists(self):
        table = Table(name='student_section', db_conn_str=DB_CONN_STR)

        self.assertEqual(type(table.db_conn), Engine)

    def test_table_rows_unique(self):
        table = Table(name='student_section', db_conn_str=DB_CONN_STR)
        field_names = ['student_id', 'term_id', 'section_id']
        uniqueness = table.rows_unique(field_names)

        self.assertTrue(uniqueness)

    def test_table_rows_not_unique(self):
        table = Table(name='student_section', db_conn_str=DB_CONN_STR)
        field_names = ['term_id']
        uniqueness = table.rows_unique(field_names)

        self.assertFalse(uniqueness)

    def test_table_count(self):
        table = Table(name='student_section', db_conn_str=DB_CONN_STR)
        ct = table.count()
        expected_ct = 4

        self.assertEqual(ct, expected_ct)

    def test_table_get_column_names(self):
        table = Table(name='student_section', db_conn_str=DB_CONN_STR)
        column_names = table.get_column_names()
        expected_column_names = ['id', 'student_id', 'term_id', 'section_id']

        self.assertEqual(column_names, expected_column_names)

    def test_table_equivalence(self):
        table_1 = Table(name='student_section', db_conn_str=DB_CONN_STR)
        table_2 = Table(name='student_section', db_conn_str=DB_CONN_STR)

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
        # TODO: Allow all classes to accept a single s_str argument or keyword args
        dataset = Table(name='a', db_conn_str=DB_CONN_STR)

        on_clause_1 = OnClause('b = c')
        join_1 = Join(kind='left', dataset=dataset, on_clause=on_clause_1)

        on_clause_2 = OnClause('c = b')
        join_2 = Join(kind='left', dataset=dataset, on_clause=on_clause_2)

        self.assertTrue(join_1.is_equivalent_to(join_2))


class FromClauseTestCase(TestCase):
    def test_from_clause_create_s_str(self):
        from_clause = FromClause(s_str='from a left join b on c = d')

        self.assertTrue(from_clause)

    def test_from_clause_create_from_dataset_joins(self):
        dataset_1 = Table(name='a')
        dataset_2 = Table(name='b')
        on_clause = OnClause('c = d')
        join = Join(kind='left', dataset=dataset_2, on_clause=on_clause)
        joins = [join]
        from_clause = FromClause(from_dataset=dataset_1, joins=joins)

        self.assertTrue(from_clause)

    def test__parse_from_clause_from_str(self):
        s_str = 'from a left join b on c = d'
        token_list = FromClause._parse_from_clause_from_str(s_str)

        self.assertEqual(type(token_list), list)

    def test__parse_from_clause_from_tokens(self):
        s_str = 'from a left join b on c = d'
        token_list = FromClause._parse_from_clause_from_str(s_str)
        from_dataset, joins = FromClause._parse_from_clause_from_tokens(token_list)

        self.assertEqual(type(from_dataset), Table)
        self.assertEqual(type(joins), list)

    def test_from_clause_is_equivalent_to(self):
        dataset_1 = Table(name='a')
        dataset_2 = Table(name='b')

        on_clause_1 = OnClause('c = d')
        join_1 = Join(kind='left', dataset=dataset_2, on_clause=on_clause_1)
        joins_1 = [join_1]

        on_clause_2 = OnClause('d = c')
        join_2 = Join(kind='left', dataset=dataset_2, on_clause=on_clause_2)
        joins_2 = [join_2]

        from_clause_1 = FromClause(from_dataset=dataset_1, joins=joins_1)
        from_clause_2 = FromClause(from_dataset=dataset_1, joins=joins_2)

        self.assertTrue(from_clause_1.is_equivalent_to(from_clause_2))

    def test_from_clause_first_join_dataset(self):
        dataset_1 = Table(name='a')
        dataset_2 = Table(name='b')
        on_clause = OnClause('c = d')
        join = Join(kind='inner', dataset=dataset_2, on_clause=on_clause)
        joins = [join]
        from_clause = FromClause(from_dataset=dataset_1, joins=joins)

        first_join_dataset = from_clause.get_first_join_dataset()

        self.assertEqual(first_join_dataset, dataset_2)

    def test_from_clause_locate_field(self):
        dataset_1 = Table(name='a')
        dataset_2 = Table(name='b')
        on_clause = OnClause('c = d')
        join = Join(kind='inner', dataset=dataset_2, on_clause=on_clause)
        joins = [join]
        from_clause = FromClause(from_dataset=dataset_1, joins=joins)

        location = from_clause.locate_field('c')
        expected_locations = [
            ('from_clause',
             'joins',
             0,
             'on_clause',
             'expression',
             'comparisons',
             0,
             'left_term')
        ]

        self.assertEqual(location, expected_locations)

    def test_from_clause_locate_field(self):
        dataset_1 = Table(name='a')
        dataset_2 = Table(name='b')
        on_clause = OnClause('c = d')
        join = Join(kind='inner', dataset=dataset_2, on_clause=on_clause)
        joins = [join]
        from_clause = FromClause(from_dataset=dataset_1, joins=joins)

        from_clause.remove_join(join)

        self.assertEqual(str(from_clause), 'from a')


class ComparisonTestCase(TestCase):
    def test_comparison_create(self):
        comparison = Comparison(s_str='a = b')

        self.assertEqual(comparison.bool_conjunction, '')
        self.assertEqual(comparison.bool_sign, '')
        self.assertEqual(comparison.left_term, 'a')
        self.assertEqual(comparison.operator, '=')
        self.assertEqual(comparison.right_term, 'b')

    def test_comparison_is_equivalent_to(self):
        comparison_1 = Comparison(s_str='a = b')
        comparison_2 = Comparison(s_str='b = a')

        self.assertTrue(comparison_1.is_equivalent_to(comparison_2))


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

    def test_where_clause_locate_field(self):
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
    def test_group_by_clause_create(self):
        field_names = ['student_id', 'term_id', 'section_id']
        group_by_clause = GroupByClause(field_names=field_names)

        self.assertTrue(group_by_clause)


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
    maxDiff = None

    def setUp(self):
        self.sql_str = ('select a name, b, fn(id, dob) age, fn(id, height) '
                        'from c join d on e = f where g = h and i = j')
        self.query = Query(sql_str=self.sql_str, db_conn_str=DB_CONN_STR)

    def test_query_create_with_db_conn_str(self):
        sql_str = ('select a name, b, fn(id, dob) age, fn(id, height) '
                   'from c join d on e = f where g = h and i = j')
        query = Query(sql_str=sql_str, db_conn_str=DB_CONN_STR)

        self.assertTrue(query)

    def test_query_create_without_db_conn_str(self):
        sql_str = ('select a name, b, fn(id, dob) age, fn(id, height) '
                   'from c join d on e = f where g = h and i = j')
        query = Query(sql_str=sql_str)

        self.assertTrue(query)

    def test_query__optional_clause_equal(self):
        sql_str_1 = ('select a name, b, fn(id, dob) age, fn(id, height) '
                     'from c join d on e = f where g = h and i = j')
        query_1 = Query(sql_str=sql_str_1)

        sql_str_2 = ('select a name, b, fn(id, dob) age, fn(id, height) '
                     'from c join d on e = f where g = h and i = j')
        query_2 = Query(sql_str=sql_str_2)

        self.assertEqual(query_1, query_2)

    def test_query_rows_unique(self):
        query = Query(sql_str='select * from student_section', db_conn_str=DB_CONN_STR)
        field_names = ['student_id', 'term_id', 'section_id']
        uniqueness = query.rows_unique(field_names)

        self.assertTrue(uniqueness)

    def test_query_rows_not_unique(self):
        query = Query(sql_str='select * from student_section', db_conn_str=DB_CONN_STR)
        field_names = ['term_id']
        uniqueness = query.rows_unique(field_names)

        self.assertFalse(uniqueness)

    def test_query_locate_column(self):
        query = Query(sql_str='select a, b from student_section')

        location = query.locate_column('b')
        expected_locations = [('select_clause', 'fields', 1)]

        self.assertEqual(location, expected_locations)

    # FUTURE: Test delete_node
    # FUTURE: Test locate_invalid_columns

    def test_query_crop(self):
        """Test ignore dangling parameters"""
        sql_str_scalarized = '''
            select subject,
                   course_number,
                   (select name from term where section.term_id = term.id) name
              from section
        '''

        query = Query(sql_str=sql_str_scalarized, db_conn_str=DB_CONN_STR)

        subquery = query.select_clause.fields[2].query
        subquery.db_conn_str = DB_CONN_STR

        actual_count = subquery.crop().count()
        expected_count = 2

        self.assertEqual(actual_count, expected_count)

    # FUTURE: Test parameterize_node

    def test_query_parameterize(self):
        sql_str_scalarized = '''
            select subject,
                   course_number,
                   (select name from term where section.term_id = term.id) name
              from section
        '''

        query = Query(sql_str=sql_str_scalarized, db_conn_str=DB_CONN_STR)

        query.select_clause.fields[2].query.db_conn_str = DB_CONN_STR

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

        query = Query(sql_str=sql_str_scalarized, db_conn_str=DB_CONN_STR)

        query.select_clause.fields[2].query.db_conn_str = DB_CONN_STR

        actual_ct = (query.select_clause.fields[2].query
                     .parameterize().run(term_id=2).count())
        expected_ct = 1

        self.assertEqual(actual_ct, expected_ct)

    def test_query_run(self):
        sql_str = '''
            select subject,
                   course_number
              from section
        '''
        query = Query(sql_str=sql_str, db_conn_str=DB_CONN_STR)

        row_dicts = query.run()
        expected_row_dicts = [
            {'subject': 'LOGC', 'course_number': '101'},
            {'subject': 'LING', 'course_number': '101'},
            {'subject': 'COMP', 'course_number': '101'},
            {'subject': 'LITR', 'course_number': '101'},
        ]

        self.assertEqual(row_dicts, expected_row_dicts)

    def test_query_count(self):
        sql_str = '''
            select subject,
                   course_number
              from section
        '''
        query = Query(sql_str=sql_str, db_conn_str=DB_CONN_STR)

        ct = query.count()
        expected_count = 4

        self.assertEqual(ct, expected_count)

    def test_query_counts(self):
        sql_str = '''
            select subject,
                   course_number
              from section
             where subject = 'LOGC'
        '''
        query = Query(sql_str=sql_str, db_conn_str=DB_CONN_STR)

        cts = query.counts()
        expected_counts = {'query': 1, 'section': 4}

        self.assertEqual(cts, expected_counts)
    
    def test_query_counts_basic(self):
        sql_str = '''
            select *
              from student
        '''

        query = Query(sql_str=sql_str, db_conn_str=DB_CONN_STR)

        actual_counts = query.counts()

        expected_counts = {
            'query': 4,
            'student': 4
        }

        self.assertEqual(actual_counts, expected_counts)

    def test_query_counts_where(self):
        sql_str = '''
            select *
              from student
             where id <= 2
        '''

        query = Query(sql_str=sql_str, db_conn_str=DB_CONN_STR)

        actual_counts = query.counts()

        expected_counts = {
            'query': 2,
            'student': 4
        }

        self.assertEqual(actual_counts, expected_counts)

    def test_query_counts_joins(self):
        sql_str = '''
            select *
              from student
              join student_section
                on student.id = student_section.student_id
        '''

        query = Query(sql_str=sql_str, db_conn_str=DB_CONN_STR)

        actual_counts = query.counts()

        expected_counts = {
            'query': 4,
            'student': 4,
            'student_section': 4
        }

        self.assertEqual(actual_counts, expected_counts)

    # FUTURE: Test rows_exist

    # FUTURE: Unskip this when fixed (the term Table wasn't getting the db_conn_str)
    def skip_test_query_scalarize(self):
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

        query = Query(sql_str=sql_str_original, db_conn_str=DB_CONN_STR)

        actual_scalarized_query = query.scalarize()
        actual_scalarized_query.db_conn_str = DB_CONN_STR

        print('baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaad')
        expected_scalarized_query = Query(sql_str=sql_str_scalarized, db_conn_str=DB_CONN_STR)
        print('baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaad')

        print(actual_scalarized_query.select_clause.fields[2].query.from_clause.__dict__)
        print('ddddddddddddddddd')
        print(expected_scalarized_query.select_clause.fields[2].query.from_clause.__dict__)

        self.assertEqual(actual_scalarized_query, expected_scalarized_query)

    def test_query_is_leaf(self):
        sql_str_scalarized = '''
            select subject,
                   course_number,
                   (select name from term where section.term_id = term.id) name
              from section
        '''

        query = Query(sql_str=sql_str_scalarized, db_conn_str=DB_CONN_STR)

        self.assertFalse(query.is_leaf())
        self.assertTrue(query.select_clause.fields[2].query.is_leaf())

    # FUTURE: Test fuse
    # FUTURE: Test bind_params

    def test_query_format_sql(self):
        sql_str = '''
            select subject,
                   course_number,
                   (select name from term where section.term_id = term.id) name
              from section
        '''

        query = Query(sql_str=sql_str, db_conn_str=DB_CONN_STR)
        expected_sql = 'select subject, course_number, (select name from term where section.term_id = term.id) name from section'

        self.assertEqual(query.format_sql(), expected_sql)

    # FUTURE: Test output_sql_file

    def test_query_subquery_str(self):
        sql_str = '''
            select subject
              from section
        '''

        query = Query(sql_str=sql_str, db_conn_str=DB_CONN_STR)
        subquery_str = query.subquery_str()
        expected_subquery_str = '(select subject from section)'

        self.assertEqual(subquery_str, expected_subquery_str)

    # FUTURE: Test filter_by_subquery

    def test_fields(self):
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
        expected_select_clause_str = (
            'select a name, b, fn(id, dob) age, fn(id, height)')
        expected_select_clause = SelectClause(expected_select_clause_str)
        actual_select_clause = self.query.select_clause

        self.assertEqual(actual_select_clause, expected_select_clause)
        self.assertEqual(str(actual_select_clause), expected_select_clause_str)

    def test_joins(self):
        join_dict = {
            'kind': 'inner',
            'dataset': Table(name='d', db_conn_str=DB_CONN_STR),
            'on_clause': OnClause(s_str='on e = f')}
        expected_joins = [Join(**join_dict)]

        actual_joins = self.query.from_clause.joins

        self.assertEqual(actual_joins, expected_joins)

        self.assertEqual(str(actual_joins[0]), 'join d on e = f')

    def test_from_clause(self):
        join_dict = {
            'kind': 'inner',
            'dataset': Table(name='d', db_conn_str=DB_CONN_STR),
            'on_clause': OnClause(s_str='on e = f')}
        expected_joins = [Join(**join_dict)]

        expected_table = Table(name='c', db_conn_str=DB_CONN_STR)
        expected_from_clause = FromClause(
            from_dataset=expected_table, joins=expected_joins)
        actual_from_clause = self.query.from_clause

        self.assertEqual(actual_from_clause, expected_from_clause)

        self.assertEqual(str(actual_from_clause), 'from c join d on e = f')

    def test_where_clause(self):
        expected_where_clause = WhereClause(s_str='where g = h and i = j')
        actual_where_clause = self.query.where_clause

        self.assertEqual(actual_where_clause, expected_where_clause)

        self.assertEqual(str(actual_where_clause), 'where g = h and i = j')

    def test_query(self):
        expected_query = Query(sql_str=self.sql_str, db_conn_str=DB_CONN_STR)
        actual_query = self.query

        self.assertEqual(actual_query, expected_query)

        self.assertEqual(str(actual_query), self.sql_str)

    def test_query_without_from_clause(self):
        sql_str = 'select 1'
        actual_query = Query(sql_str=sql_str)

        self.assertEqual(str(actual_query), sql_str)

    def test_complex_query(self):
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

        query = Query(sql_str=sql_str)
        expected_query = Query(sql_str=sql_str)
        actual_query = query

        self.assertEqual(actual_query, expected_query)

        expected_query_str = (
            "select a name, b, fn(id, dob) age, fn(id, height), (select c1 "
            "from a1 where a1.b1 = b) c1 from c join d on e = f left join "
            "(select shape from k where kind = 'quadrilateral') on l = m and "
            "n = o where g = h and i = j")

        self.assertEqual(actual_query.__str__(), expected_query_str)


class FieldTestCase(TestCase):
    def test_field_str_normal(self):
        field = Field('exp a')

        self.assertTrue(field)
        self.assertEqual(field.expression, 'exp')
        self.assertEqual(field.alias, 'a')

    def test_field_list_normal(self):
        field = Field('exp a')

        self.assertTrue(field)
        self.assertEqual(field.expression, 'exp')
        self.assertEqual(field.alias, 'a')

    def test_field_str_subquery(self):
        field = Field('(select fld from tbl) a')

        self.assertTrue(field)
        self.assertEqual(field.expression, '(select fld from tbl)')
        self.assertEqual(field.alias, 'a')
        self.assertEqual(type(field.query), Query)

    def test_field_list_subquery(self):
        field = Field('(select fld from tbl) a')

        self.assertTrue(field)
        self.assertEqual(field.expression, '(select fld from tbl)')
        self.assertEqual(field.alias, 'a')
        self.assertEqual(type(field.query), Query)

    def test_field_no_subquery(self):
        field = Field('column_name a')

        self.assertTrue(field)
        self.assertEqual(field.expression, 'column_name')
        self.assertEqual(field.alias, 'a')
        self.assertEqual(field.query, Query())


# FUTURE: Test UpdateClause


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


class UpdateStatementTestCase(TestCase):
    def test_update_statement_basic(self):
        sql_str = "update student set major = 'BIOL' where id = 4"

        update_clause = UpdateClause('update student')
        set_clause = SetClause(s_str="set major = 'BIOL'")
        where_clause = WhereClause(s_str='where id = 4')

        expected_update_statement = UpdateStatement(
            update_clause=update_clause, set_clause=set_clause,
            where_clause=where_clause)

        self.assertEqual(sql_str, str(expected_update_statement))

    def test_update_statement_count(self):
        sql_str = "update student set major = 'BIOL' where id = 4"

        update_statement = UpdateStatement(s_str=sql_str, db_conn_str=DB_CONN_STR)

        actual_expected_row_count = update_statement.count()
        expected_expected_row_count = 1

        self.assertEqual(actual_expected_row_count,
                         expected_expected_row_count)


# FUTURE: Test DeleteClause


class DeleteStatementTestCase(TestCase):
    def test_delete_statement_basic(self):
        sql_str = "delete from student where id = 4"

        delete_clause = DeleteClause()
        from_clause = FromClause('from student')
        where_clause = WhereClause(s_str='where id = 4')

        expected_delete_statement = DeleteStatement(
            delete_clause=delete_clause, from_clause=from_clause,
            where_clause=where_clause)

        self.assertEqual(sql_str, str(expected_delete_statement))

    def test_delete_statement_count(self):
        sql_str = "delete from student where id = 4"

        delete_statement = DeleteStatement(s_str=sql_str, db_conn_str=DB_CONN_STR)

        actual_expected_row_count = delete_statement.count()
        expected_expected_row_count = 1

        self.assertEqual(actual_expected_row_count,
                         expected_expected_row_count)


# TODO: Test get_dataset
# TODO: Test parse_select_clause
# TODO: Test parse_field
# TODO: Test parse_fields
# TODO: Test parse_fields_from_token_list

# FUTURE: See if those last module-level functions could be static methods
