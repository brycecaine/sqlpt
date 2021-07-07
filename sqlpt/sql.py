from copy import deepcopy
from dataclasses import dataclass
from sqlite3 import connect

import pandas as pd
import sqlparse
from sqlalchemy import create_engine
from sqlparse.sql import Comparison as SQLParseComparison
from sqlparse.sql import Identifier, Parenthesis, Where

from sqlpt.service import (get_field_alias, get_field_expression,
                           get_field_strs, get_join_kind, is_equivalent,
                           is_join, is_sqlparse_comparison, remove_whitespace,
                           remove_whitespace_from_str)


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
    expression: str
    alias: str

    def __hash__(self):
        return hash(str(self))

    def __init__(self, *args):
        if len(args) == 1:
            field_str = args[0]
            self.expression = get_field_expression(field_str)
            self.alias = get_field_alias(field_str)

        elif len(args) == 2:
            self.expression = args[0]
            self.alias = args[1]

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

    def add_field(self, *args):
        field = Field(*args)
        self.fields.append(field)

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

    def __init__(self, *args):
        if len(args) == 1:
            sql_str = args[0]
            sql_elements = sqlparse.parse(sql_str)

            start_appending = False
            from_clause_tokens = []

            sql_tokens = remove_whitespace(sql_elements[0].tokens)

            for sql_token in sql_tokens:
                if not sql_token.is_whitespace:
                    if sql_token.value == 'from':
                        start_appending = True

                    if sql_token.value[:5] == 'where':
                        start_appending = False

                    if start_appending:
                        from_clause_tokens.append(sql_token)

            from_table = None
            joins = []

            if from_clause_tokens:
                # Remove 'from' keyword for now
                from_clause_tokens.pop(0)

                # Get from_table
                from_table_name = str(from_clause_tokens.pop(0))
                # May need to handle subquery; it will be a Parenthesis object
                from_table = Table(from_table_name)

                # Construct joins
                kind = None
                dataset = None
                comparisons = []

                for item in from_clause_tokens:
                    # Parse join item
                    item_is_join = is_join(item)

                    if item_is_join:
                        # Create join object with previously populated values
                        # if applicable, and clear out values for a next one
                        if kind and dataset and comparisons:
                            join_kind = deepcopy(str(kind))
                            join_dataset = deepcopy(str(dataset))
                            join_comparisons = deepcopy(comparisons)

                            join = Join(
                                join_kind, join_dataset, join_comparisons)
                            joins.append(join)

                            kind = None
                            dataset = None
                            comparisons = []

                        kind = get_join_kind(item)

                        continue

                    # Parse dataset item
                    if type(item) == Identifier:
                        dataset = Table(str(item))

                        continue

                    if type(item) == Parenthesis:
                        sql_str = str(item)[1:-1]
                        dataset = Query(sql_str)

                        continue

                    # Parse comparison item
                    item_is_sqlparse_comparison = is_sqlparse_comparison(item)

                    if item_is_sqlparse_comparison:
                        comparison = Comparison(item.__str__())
                        comparisons.append(comparison)

                        continue

                # Create the last join
                if kind and dataset and comparisons:
                    join = Join(kind, dataset, comparisons)
                    joins.append(join)

            self.from_table = from_table
            self.joins = joins

        elif len(args) == 2:
            self.from_table = args[0]
            self.joins = args[1]

    def __str__(self):
        from_clause_str = ''

        if self.from_table:
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
    left_term: str
    operator: str
    right_term: str

    def __hash__(self):
        return hash(str(self))

    def __init__(self, *args):
        if len(args) == 1:
            comparison_str = args[0]
            statement = sqlparse.parse(comparison_str)
            sqlparse_comparison = statement[0].tokens[0]
            comparison_tokens = remove_whitespace(sqlparse_comparison.tokens)

            elements = [comparison_token.value
                        for comparison_token in comparison_tokens]

        elif len(args) == 3:
            elements = args

        self.left_term = elements[0]
        self.operator = elements[1]
        self.right_term = elements[2]

        """ TODO: See if this is needed (where_token is a SQLParseComparison):
        comparison_tokens = remove_whitespace(
            where_token.tokens)

        for i, c_token in enumerate(comparison_tokens):
            if type(c_token) in (Identifier, Parenthesis):
                left_term = c_token.value
                operator = comparison_tokens[i+1].value
                right_term = comparison_tokens[i+2].value

                comparison = Comparison(left_term, operator, right_term)

                comparisons.append(comparison)
                break
        """

    def is_equivalent_to(self, other):
        equivalent = False

        if isinstance(other, self.__class__):
            if self.operator == '=':
                operator_equivalent = self.operator == other.operator

                expressions_equivalent = (
                    {self.left_term, self.right_term} ==
                    {other.left_term, other.right_term})

            if operator_equivalent and expressions_equivalent:
                equivalent = True

        return equivalent

    def __str__(self):
        string = f'{self.left_term} {self.operator} {self.right_term}'

        return string


@dataclass
class WhereClause:
    comparisons: list

    def __hash__(self):
        return hash(str(self))

    def __init__(self, *args):
        if type(args[0]) == str:
            sql_str = args[0]
            sql_elements = sqlparse.parse(sql_str)

            comparisons = []

            for sql_token in sql_elements[0].tokens:
                if type(sql_token) == Where:
                    where_tokens = sql_token.tokens

                    for where_token in where_tokens:
                        if type(where_token) == SQLParseComparison:
                            comparison = Comparison(where_token.__str__())
                            comparisons.append(comparison)

            self.comparisons = comparisons

        elif type(args[0]) == list:
            self.comparisons = args[0]

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
            left_term = comparison.left_term

            if left_term in parameter_fields:
                comparison.right_term = f':{left_term}'

        return self

    def fuse(self, where_clause):
        for self_comparison in self.comparisons:
            for other_comparison in where_clause.comparisons:
                if self_comparison.left_term == other_comparison.left_term:
                    # TODO: Figure out what to do with inequalities
                    # right_term_list.append
                    pass

        return self

    def add_comparison(self, comparison):
        self.comparisons.append(comparison)


@dataclass
class Query(DataSet):
    select_clause: SelectClause
    from_clause: FromClause
    where_clause: WhereClause

    def __hash__(self):
        return hash(str(self))

    def __init__(self, sql_str):
        sql_str = remove_whitespace_from_str(sql_str)

        self.select_clause = SelectClause(sql_str)
        self.from_clause = FromClause(sql_str)
        self.where_clause = WhereClause(sql_str)

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
        df = pd.read_sql_query(self.__str__(), self.db_conn)

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
            bound_sql_str = self.__str__().replace(f':{key}', str(value))
            self.__init__(bound_sql_str)

        return self

    def format_sql(self):
        formatted_sql = sqlparse.format(self.__str__())

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

    def subquery_str(self):
        string = f'({self.__str__()})'

        return string

    def add_subquery_field(self, subquery_str, alias):
        self.select_clause.add_field(subquery_str, alias)

    def filter_by_subquery(self, subquery_str, operator, value):
        if type(value) == list:
            if operator == '=':
                operator = 'in'

            value = ','.join(f"{item}" for item in value if item)
            value = f'({value})'

        comparison = Comparison(subquery_str, operator, value)

        self.where_clause.add_comparison(comparison)
