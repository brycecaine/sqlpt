from copy import deepcopy
from dataclasses import dataclass
from sqlite3 import connect

import pandas as pd
import sqlparse
from sqlalchemy import create_engine
from sqlparse.sql import Comparison as SQLParseComparison
from sqlparse.sql import Identifier, Parenthesis, Token, Where

from sqlpt.service import (get_field_alias, get_field_expression,
                           is_join, is_dataset, is_comparison, get_field_strs, is_equivalent, remove_whitespace)


@dataclass
class DataSet:
    pass


@dataclass
class Table(DataSet):
    name: str

    def __hash__(self):
        return hash(str(self))

    def __str__(self):
        return self.name

    def is_equivalent_to(self, other):
        equivalent = False

        if isinstance(other, self.__class__):
            equivalent = self.name == other.name

        return equivalent


@dataclass
class Field:
    field_str: str
    expression: str
    alias: str

    def __hash__(self):
        return hash(str(self))

    def __init__(self, field_str):
        self.field_str = field_str
        self.expression = get_field_expression(field_str)
        self.alias = get_field_alias(field_str)

    def __str__(self):
        alias = f' {self.alias}' if self.alias else ''
        description = f'{self.expression}{alias}'

        return description


@dataclass
class SelectClause:
    fields: list

    def __hash__(self):
        return hash(str(self))

    def __init__(self, select_clause_str):
        field_strs = get_field_strs(select_clause_str)

        # TODO: Convert to list comprehension (multi-line)
        self.fields = []

        for field_str in field_strs:
            self.fields.append(Field(field_str))

    def __str__(self):
        select_clause_str = ''

        for i, field in enumerate(self.fields):
            if i == 0:
                select_clause_str += f'select {field}'

            else:
                select_clause_str += f', {field}'

        return select_clause_str

    def is_equivalent_to(self, other):
        equivalent = False

        if isinstance(other, self.__class__):
            equivalent = set(self.fields) == set(other.fields)

        return equivalent

    def fuse(self, select_clause):
        pass


@dataclass
class Join:
    kind: str
    dataset: DataSet
    comparisons: list

    def __hash__(self):
        return hash(str(self))

    def __str__(self):
        join_str = f' {self.kind} {self.dataset}'

        for i, comparison in enumerate(self.comparisons):
            conjunction = 'on' if i == 0 else 'and'
            join_str += f' {conjunction} {comparison}'

        return join_str

    def is_equivalent_to(self, other):
        equivalent = False

        if isinstance(other, self.__class__):
            datasets_equivalent = is_equivalent(
                [self.dataset], [other.dataset])

            comparison_equivalent = is_equivalent(
                self.comparisons, other.comparisons)

            equivalent = datasets_equivalent and comparison_equivalent

        return equivalent


@dataclass
class FromClause:
    from_table: Table
    joins: list

    def __hash__(self):
        return hash(str(self))

    def __str__(self):
        from_clause_str = f'from {self.from_table}'

        for join in self.joins:
            from_clause_str += f'{join}'

        return from_clause_str

    def is_equivalent_to(self, other):
        equivalent = False

        if isinstance(other, self.__class__):
            # TODO: Allow for equivalence if tables and comparisons are out of
            # order
            equivalent = is_equivalent(self.joins, other.joins)

        return equivalent


@dataclass
class Comparison:
    # TODO: Figure out a new name for "expression" (maybe "value"?)
    left_expression: str
    operator: str
    right_expression: str

    def __hash__(self):
        return hash(str(self))

    def is_equivalent_to(self, other):
        equivalent = False

        if isinstance(other, self.__class__):
            if self.operator == '=':
                operator_equivalent = self.operator == other.operator

                expressions_equivalent = (
                    {self.left_expression, self.right_expression} ==
                    {other.left_expression, other.right_expression})

            if operator_equivalent and expressions_equivalent:
                equivalent = True

        return equivalent

    def __str__(self):
        comparison_str = (
            f'{self.left_expression} {self.operator} {self.right_expression}')

        return comparison_str


@dataclass
class WhereClause:
    comparisons: list

    def __hash__(self):
        return hash(str(self))

    def is_equivalent_to(self, other):
        equivalent = False

        if isinstance(other, self.__class__):
            for i, join in enumerate(self.comparisons):
                if join == other.comparisons[i]:
                    equivalent = True

        return equivalent

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


def get_join_kind(item):
    join_kind = 'join'

    if type(item) == Token and 'left' in str(item).lower():
        join_kind = 'left join'

    return join_kind


def get_dataset(item):
    if type(item) == Identifier:
        dataset = Table(str(item))

    elif type(item) == Parenthesis:
        sql_str = str(item)[1:-1]
        dataset = SubQuery(sql_str)

    return dataset


def get_comparison(item):
    token_str_list = []

    comparison_tokens = remove_whitespace(item.tokens)

    for comparison_token in comparison_tokens:
        token_str_list.append(comparison_token.value)

    comparison = Comparison(*token_str_list)

    return comparison


def get_select_clause(sql_str):
    select_clause = SelectClause(sql_str)

    return select_clause


def get_from_clause(sql_str):
    sql_elements = sqlparse.parse(sql_str)

    start_appending = False
    from_clause_tokens = []

    sql_tokens = remove_whitespace(sql_elements[0].tokens)

    from_clause = None

    for sql_token in sql_tokens:
        if not sql_token.is_whitespace:
            if sql_token.value == 'from':
                start_appending = True

            if sql_token.value[:5] == 'where':
                start_appending = False

            if start_appending:
                from_clause_tokens.append(sql_token)

    if from_clause_tokens:
        # Remove 'from' keyword for now
        from_clause_tokens.pop(0)

        # Get from_table
        from_table_name = str(from_clause_tokens.pop(0))
        from_table = Table(from_table_name)

        # Construct joins
        joins = []

        kind = None
        dataset = None
        comparisons = []

        for item in from_clause_tokens:
            # Parse join item
            item_is_join = is_join(item)

            if item_is_join:
                # Create join object with previously populated values if
                # applicable, and clear out values for a next one
                if kind and dataset and comparisons:
                    join_kind = deepcopy(str(kind))
                    join_dataset = deepcopy(str(dataset))
                    join_comparisons = deepcopy(comparisons)

                    join = Join(join_kind, join_dataset, join_comparisons)
                    joins.append(join)

                    kind = None
                    dataset = None
                    comparisons = []

                kind = get_join_kind(item)

                continue

            # Parse dataset item
            item_is_dataset = is_dataset(item)

            if item_is_dataset:
                dataset = get_dataset(item)

                continue

            # Parse comparison item
            item_is_comparison = is_comparison(item)

            if item_is_comparison:
                comparison = get_comparison(item)
                comparisons.append(comparison)

                continue

        # Create the last join
        if kind and dataset and comparisons:
            join = Join(kind, dataset, comparisons)
            joins.append(join)

        from_clause = FromClause(from_table, joins)

    return from_clause


def get_where_clause(sql_str):
    sql_elements = sqlparse.parse(sql_str)

    comparisons = []

    for sql_token in sql_elements[0].tokens:
        if type(sql_token) == Where:
            where_tokens = sql_token.tokens

            for where_token in where_tokens:
                if type(where_token) == SQLParseComparison:
                    comparison_tokens = remove_whitespace(
                        where_token.tokens)

                    for i, c_token in enumerate(comparison_tokens):
                        if type(c_token) in (Identifier, Parenthesis):
                            left_expression = c_token.value
                            operator = comparison_tokens[i+1].value
                            right_expression = comparison_tokens[i+2].value

                            comparison = Comparison(
                                left_expression, operator,
                                right_expression)

                            comparisons.append(comparison)
                            break

    where_clause = WhereClause(comparisons)

    return where_clause


@dataclass
class Query(DataSet):
    sql_str: str
    select_clause: SelectClause
    from_clause: FromClause
    where_clause: WhereClause

    def __hash__(self):
        return hash(str(self))

    def __init__(self, sql_str):
        self.sql_str = sql_str
        self.select_clause = get_select_clause(sql_str)
        self.from_clause = get_from_clause(sql_str)
        self.where_clause = get_where_clause(sql_str)

    def __str__(self):
        string = (f'{self.select_clause} {self.from_clause} '
                  f'{self.where_clause}')

        return string


    @property
    def db_conn(self):
        self.db_conn = create_engine('sqlite:///sqlpt/college.db')

        return self.db_conn

    @property
    def dataframe(self):
        df = pd.read_sql_query(self.sql_str, self.db_conn)

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

    def bind_params(self, **kwargs):
        for key, value in kwargs.items():
            bound_sql_str = self.sql_str.replace(f':{key}', str(value))
            self.__init__(bound_sql_str)

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


@dataclass
class SubQuery(Query):
    def __str__(self):
        description = (f'({self.select_clause} {self.from_clause} '
                       f'{self.where_clause})')

        return description


# ------------------------------
# bluberry or clear blu or blu flame or blu blazes

@dataclass
class LogicUnit:
    name: str
    query: Query
    db_source: str

    def run_sql(self):
        conn = connect(self.db_source)
        curs = conn.cursor()

        # TODO: Change this to self.query.sql_str() (or property)
        sql_str = self.query.__str__()
        curs.execute(sql_str)

        result = curs.fetchall()

        if not self.query.from_clause:  # Scalar result
            result = result[0][0]

        conn.close()

        return result

    def get_population(self):
        population = self.run_sql()

        return population

    def get_value(self, **kwargs):
        self.query.bind_params(**kwargs)

        value = self.run_sql()

        return value
