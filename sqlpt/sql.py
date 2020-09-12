from sqlalchemy import create_engine
from sqlparse.sql import (
    Comparison as SQLParseComparison, Identifier, IdentifierList, Where)
import pandas as pd
import sqlparse


class Table:
    def __init__(self, name):
        self.name = name

    def __str__(self):
        return self.name

    def __eq__(self, other):
        equal = False
        if isinstance(other, self.__class__):
            equal = self.name == other.name

        return equal


class Field:
    def __init__(self, name):
        self.name = name

    def __str__(self):
        description = str(self.name)

        return description

    def __eq__(self, other):
        equal = False

        if isinstance(other, self.__class__):
            equal = self.name == other.name

        return equal


class SelectClause:
    def __init__(self, fields):
        self.fields = fields

    def __str__(self):
        select_clause_str = ''

        for i, field in enumerate(self.fields):
            if i == 0:
                select_clause_str += f'select {field}'

            else:
                select_clause_str += f', {field}'

        return select_clause_str

    def __eq__(self, other):
        equal = False

        if isinstance(other, self.__class__):
            for i, field in enumerate(self.fields):
                if field == other.fields[i]:
                    equal = True

        return equal

    def fuse(self, select_clause):
        pass


class Join:
    def __init__(self, left_table=None, right_table=None, comparisons=None):
        self.left_table = left_table
        self.comparisons = comparisons
        self.right_table = right_table

    def __str__(self):
        left_side_str = f'from {self.left_table}'
        right_side_str = f' join {self.right_table}'

        for i, comparison in enumerate(self.comparisons):
            if i == 0:
                right_side_str += f' on {comparison}'

            else:
                right_side_str += f' and {comparison}'

        if not self.right_table:
            join_str = left_side_str

        elif not self.left_table:
            join_str = right_side_str

        else:
            join_str = f'{left_side_str}{right_side_str}'

        return join_str

    def __eq__(self, other):
        equal = False

        if isinstance(other, self.__class__):
            left_tables_equal = self.left_table == other.left_table
            right_tables_equal = self.right_table == other.right_table

            tables_equal = left_tables_equal and right_tables_equal

            if not tables_equal:
                left_equals_right = self.left_table == other.right_table
                right_equals_left = self.right_table == other.left_table

                tables_equal = left_equals_right and right_equals_left

            comparison_equal = (self.comparisons == other.comparisons)

            equal = tables_equal and comparison_equal

        return equal


class FromClause:
    def __init__(self, joins):
        self.joins = joins

    def __str__(self):
        from_clause_str = ''

        for join in self.joins:
            from_clause_str += f'{join}'

        return from_clause_str

    def __eq__(self, other):
        equal = False

        if isinstance(other, self.__class__):
            for i, join in enumerate(self.joins):
                if join == other.joins[i]:
                    equal = True

        return equal


class Comparison:
    def __init__(self, left_expression, operator, right_expression):
        self.left_expression = left_expression
        self.operator = operator
        self.right_expression = right_expression

    def __eq__(self, other):
        equal = False

        if isinstance(other, self.__class__):
            left_equal = self.left_expression == other.left_expression
            operator_equal = self.operator == other.operator
            right_equal = self.right_expression == other.right_expression

            if left_equal and operator_equal and right_equal:
                equal = True

        return equal

    def __str__(self):
        comparison_str = (
            f'{self.left_expression} {self.operator} {self.right_expression}')

        return comparison_str


class WhereClause:
    def __init__(self, comparisons):
        self.comparisons = comparisons

    def __eq__(self, other):
        equal = False

        if isinstance(other, self.__class__):
            for i, join in enumerate(self.comparisons):
                if join == other.comparisons[i]:
                    equal = True

        return equal

    def __str__(self):
        where_clause_str = ''

        for i, comparison in enumerate(self.comparisons):
            if i == 0:
                where_clause_str += f'where {comparison}'

            else:
                where_clause_str += f' and {comparison}'

        return where_clause_str

    def parameterize(self, parameter_fields):
        for comparison in self.comparisons:
            left_expression = comparison.left_expression

            if left_expression in parameter_fields:
                comparison.right_expression = f':{left_expression}'

        return self

    def fuse(self, where_clause):
        for self_comparison in self.comparisons:
            for other_comparison in where_clause.comparisons:
                if (self_comparison.left_expression ==
                        other_comparison.left_expression):
                    # TODO: Left off here
                    # TODO: What to do with inequalities
                    # right_expression_list.append
                    pass

        return self


class Query:
    def __init__(self, sql_str, db_str):
        self.db_str = db_str
        self.sql_str = sql_str

    def __str__(self):
        description = (f'{self.select_clause} {self.from_clause} '
                       f'{self.where_clause}')

        return description

    def remove_whitespace(self, token_list):
        tokens = [x for x in token_list if not x.is_whitespace]

        return tokens

    def tokenize(self):
        sql_statement = sqlparse.parse(self.sql_str)
        all_tokens = sql_statement[0].tokens
        tokens = self.remove_whitespace(all_tokens)

        return tokens

    @property
    def select_clause(self):
        fields = []

        sql_elements = sqlparse.parse(self.sql_str)

        for sql_token in sql_elements[0].tokens:
            if type(sql_token) == IdentifierList:
                for identifier in sql_token:
                    field_name = str(identifier)
                    if field_name not in (',', ' '):
                        field = Field(field_name)
                        fields.append(field)

        select_clause = SelectClause(fields)

        return select_clause

    @property
    def from_clause(self):
        sql_elements = sqlparse.parse(self.sql_str)

        start_appending = False
        from_clause_tokens = []

        for sql_token in sql_elements[0].tokens:
            if not sql_token.is_whitespace:
                if sql_token.value == 'from':
                    start_appending = True

                if sql_token.value[:5] == 'where':
                    start_appending = False

                if start_appending:
                    from_clause_tokens.append(sql_token)

        from_clause_tokens.pop(0)
        table_name = str(from_clause_tokens.pop(0))
        first_table = Table(table_name)

        joins = []

        for i, item in enumerate(from_clause_tokens):
            if str(item) == 'join':
                # Determine left table if applicable
                left_table = None

                if i == 0:
                    left_table = first_table

                # Determine right table
                right_table = Table(str(from_clause_tokens[i+1]))

                # Determine comparisons
                j = 2
                comparisons = []

                while True:
                    try:
                        token = from_clause_tokens[i+j]

                    except IndexError:
                        break

                    if str(token) in ('on', 'and'):
                        comparison = from_clause_tokens[i+j+1]
                        comparisons.append(comparison)

                    else:
                        break

                    j += 2

                join = Join(left_table, right_table, comparisons)

                joins.append(join)

        from_clause = FromClause(joins)

        return from_clause

    @property
    def where_clause(self):
        sql_elements = sqlparse.parse(self.sql_str)

        comparisons = []

        for sql_token in sql_elements[0].tokens:
            if type(sql_token) == Where:
                where_tokens = sql_token.tokens

                for where_token in where_tokens:
                    if type(where_token) == SQLParseComparison:

                        comparison_tokens = self.remove_whitespace(
                            where_token.tokens)

                        for i, c_token in enumerate(comparison_tokens):
                            if type(c_token) == Identifier:
                                left_expression = c_token.value
                                operator = comparison_tokens[i+1].value
                                right_expression = comparison_tokens[i+2].value

                                comparison = Comparison(
                                    left_expression, operator,
                                    right_expression)

                                comparisons.append(comparison)

        where_clause = WhereClause(comparisons)

        return where_clause

    @property
    def dataframe(self):
        engine = create_engine(self.db_str)
        df = pd.read_sql_query(self.sql_str, engine)

        return df

    def fuse(self, query):
        # TODO: Figure out how to fuse from clauses
        if self.from_clause == query.from_clause:
            self.select_clause.fuse(query.select_clause)
            self.where_clause.fuse(query.where_clause)

        return self

    def parameterize(self, parameter_fields):
        self.where_clause.parameterize(parameter_fields)

        return self

    def format_sql(self):
        formatted_sql = sqlparse.format(self.sql_str)

        return formatted_sql

    def describe(self):
        description = self.dataframe.describe()

        return description

    def head(self, n=5):
        head_data = self.dataframe.head(n)

        return head_data

    def output_sql_file(self, path):
        with open(path, 'wt') as sql_file:
            sql_file.write(self.format_sql())

    def output_data_file(self, path):
        self.dataframe.to_csv(path, index=False)
