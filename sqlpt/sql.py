import re
from copy import deepcopy
from dataclasses import dataclass
from socket import SocketKind

import pandas as pd
import sqlparse
import ttg
from sqlalchemy import create_engine
from sqlparse.sql import Identifier, Parenthesis, Token, Where

from sqlpt.service import (
    get_field_alias, get_field_expression,
    get_field_strs, get_join_kind, is_equivalent,
    is_join, is_sqlparse_comparison,
    remove_commas, remove_whitespace, remove_whitespace_from_str)


def get_sql_clauses(sql_str):
    sql_tokens = remove_whitespace(sqlparse.parse(sql_str)[0].tokens)

    select_clause_token_list = []
    from_clause_token_list = []
    where_clause_token_list = []

    clause_token_list = select_clause_token_list

    for sql_token in sql_tokens:
        if type(sql_token) == Token and sql_token.value.lower() == 'from':
            clause_token_list = from_clause_token_list

        elif type(sql_token) == Where:
            clause_token_list = where_clause_token_list

        clause_token_list.append(sql_token)

    return select_clause_token_list, from_clause_token_list, where_clause_token_list


def get_select_clause_str(sql_str):
    from_clause_str = sql_str.split(' from ')[0]

    return from_clause_str


def parse_fields(token_list):
    fields = []

    regex = r'(?P<expression>\w+(?:\([^\)]*\))?|\([^\)]*\))[ ]?(?P<alias>\w*)'
    pattern = re.compile(regex)

    # TODO: Chain the "remove" functionality
    print('ttttttttttttttttttttt')
    print(token_list)
    print(token_list[1])
    for identifier in remove_commas(remove_whitespace(token_list[1])):
        match_obj = re.match(pattern, str(identifier))
        field_args = (match_obj.group('expression'), match_obj.group('alias'))
        field = Field(*field_args)
        fields.append(field)

    return fields


def parse_fields_from_str(sql_snip):
    regex = r'(?P<expression>\w+(?:\([^\)]*\))?|\([^\)]*\))[ ]?(?P<alias>\w*)'

    pattern = re.compile(regex)

    fields = []

    for match in re.finditer(pattern, sql_snip):
        field = Field(match.group('expression'), match.group('alias'))
        fields.append(field)

    return fields


def parse_field(sql_snip):
    regex = r'(\w*)?(\(.*\))?[ ]*(\w*)'
    match_obj = re.match(regex, sql_snip, re.M|re.I)

    expression_part_1 = match_obj.group(1)
    expression_part_2 = match_obj.group(2)

    if expression_part_2:
        expression = f'{expression_part_1}{expression_part_2}'
    else:
        expression = expression_part_1

    alias = match_obj.group(3)
    field = Field(expression, alias)

    return field


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

    def __init__(self, *args):
        if type(args[0]) == list:
            token_list = args[0]
            self.fields = parse_fields(token_list)

        else:
            select_clause_str = args[0]
            field_strs = get_field_strs(select_clause_str)
            self.fields = [Field(field_str) for field_str in field_strs]

    def __str__(self):
        select_clause_str = f"select {', '.join(self.field_strs)}"

        return select_clause_str

    @property
    def field_strs(self):
        field_strs = [str(field) for field in self.fields]

        return field_strs

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


def get_expression(sql_tokens):
    expression = ''

    for sql_token in sql_tokens:
        trimmed_sql_token = ' '.join(str(sql_token).split())
        expression += f'{trimmed_sql_token} '

    expression = expression[:-1]

    return expression


def get_truth_table_result(expr):
    expr_w_parens = re.sub(r'(\w+\s*=\s*\w+)', r'(\1)', expr)
    inputs = [i.replace(' ', '') for i in re.split(r'=|and|or|not', expr)]
    truth_table = ttg.Truths(inputs, [expr_w_parens])

    truth_table_result = []

    for conditions_set in truth_table.base_conditions:
        condition_result = truth_table.calculate(*conditions_set)
        truth_table_result.append(condition_result[-1])

    return truth_table_result


def is_equiv(expr_1, expr_2):
    equiv = get_truth_table_result(expr_1) == get_truth_table_result(expr_2)

    return equiv


@dataclass
class ExpressionClause:
    leading_word: str
    expression: str

    def __hash__(self):
        return hash(str(self))

    def __init__(self, *args):
        full_expression = args[0]
        full_expression_words = full_expression.split(' ')
        self.leading_word = full_expression_words.pop(0)
        self.expression = ' '.join(full_expression_words)

    def is_equivalent_to(self, other):
        equivalent = is_equiv(self.expression, other.expression)

        return equivalent

    def __str__(self):
        return f'{self.leading_word} {self.expression}'

    def parameterize(self, parameter_fields):
        raise Exception('Needs implementation')
        # TODO Start with matching the comparison, kind of like this:
        # expr_w_parens = re.sub(r'(\w+\s*=\s*\w+)', r'(\1)', expr)
        """
        for comparison in self.comparisons:
            left_term = comparison.left_term

            if left_term in parameter_fields:
                comparison.right_term = f':{left_term}'

        return self
        """


class OnClause(ExpressionClause):
    pass


@dataclass
class Join:
    kind: str
    dataset: DataSet
    on_clause: OnClause

    def __hash__(self):
        return hash(str(self))

    def __str__(self):
        dataset_str = f'({self.dataset})' if isinstance(self.dataset, Query) else self.dataset
        join_str = f' {self.kind} {dataset_str} {self.on_clause}'

        return join_str

    def is_equivalent_to(self, other):
        equivalent = (self.kind == other.kind
                      and
                      is_equivalent([self.dataset], [other.dataset])
                      and
                      self.on_clause.is_equivalent_to(other.on_clause))

        return equivalent


@dataclass
class FromClause:
    # TODO: Accommodate DataSet here, not just Table instances
    from_table: Table
    joins: list

    def __hash__(self):
        return hash(str(self))

    # TODO Move away from sqlparse in favor of regex's here and elsewhere
    def __init__new_now_old(self, *args):
        sql_str = args[0]
        # query_sql = 'select hi, yo from dual join laud on a = b WHERE i = j'
        # query_sql = 'select hi from dual'
        # query_regex = r'/(select\s+.+\s+)(from\s+.*?\s+)(where\s+.+|$);?/i'
        query_regex = r'(select\s+.*?\s+)(from\s+.*?\s*)(where\s+.+|$);?'
        # query_regex = r'(.*)'
        match_obj = re.match(query_regex, sql_str, re.M|re.I)

        if match_obj:
            select_clause_sql = match_obj.group(1)
            from_clause_sql = match_obj.group(2)
            where_clause_sql = match_obj.group(3)

    def __init__(self, *args):
        if len(args) == 1:
            from_clause_tokens = args[0]
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
                on_tokens = []

                for item in from_clause_tokens:
                    # Parse join item
                    if is_join(item):
                        # Create join object with previously populated values
                        # if applicable, and clear out values for a next one
                        if kind and dataset and on_tokens:
                            join_kind = deepcopy(str(kind))
                            join_dataset = deepcopy(str(dataset))
                            expression = get_expression(on_tokens)
                            join_on_clause = OnClause(expression)

                            join = Join(
                                join_kind, join_dataset, join_on_clause)
                            joins.append(join)

                            kind = None
                            dataset = None
                            on_tokens = []

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
                    # TODO Left off here wrestling with whitespace
                    on_tokens.append(item)

                # Create the last join
                if kind and dataset and on_tokens:
                    expression = get_expression(on_tokens)
                    on_clause = OnClause(expression)

                    join = Join(kind, dataset, on_clause)
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

    def first_join_dataset(self):
        first_join = self.joins[0]

        if first_join.kind == 'join':
            first_join_dataset = first_join.dataset
        else:
            first_join_dataset = None

        return first_join_dataset

    def is_equivalent_to(self, other):
        equivalent = False

        if isinstance(other, self.__class__):
            # TODO: Allow for equivalence if tables and comparisons are out of
            # order

            equivalent = (self.from_table == other.from_table
                          or
                          (self.from_table == other.first_join_dataset()
                           and
                           other.from_table == self.first_join_dataset()))

            # TODO: Work this out
            if equivalent:
                for self_join in self.joins:
                    for other_join in other.joins:
                        if not self_join.is_equivalent_to(other_join):
                            equivalent = False
                            break

                for other_join in other.joins:
                    for self_join in self.joins:
                        if not other_join.is_equivalent_to(self_join):
                            equivalent = False
                            break

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


class WhereClause(ExpressionClause):
    pass


@dataclass
class Query(DataSet):
    select_clause: SelectClause
    from_clause: FromClause
    where_clause: WhereClause

    def __hash__(self):
        return hash(str(self))

    def __init__(self, sql_str):
        if sql_str:
            select_clause_tl, from_clause_tl, where_clause_tl = get_sql_clauses(sql_str)

            # TODO Make sure to get the correct where clause when parsing sql with
            #     more than one
            # TODO Figure out why leaving self.expression as '' results in:
            #     SyntaxWarning: null string passed to Literal; use Empty() instead
            # TODO Make this into a function and find a better place or it
            # TODO Allow constructing clauses by either a token list or sql str?
            self.select_clause = SelectClause(select_clause_tl)
            self.from_clause = FromClause(from_clause_tl)
            where_expression = get_expression(where_clause_tl)
            self.where_clause = WhereClause(where_expression)

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
        # FUTURE: Figure out how to fuse from clauses
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
