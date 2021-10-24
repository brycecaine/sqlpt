import re
from copy import deepcopy
from dataclasses import dataclass

import pandas as pd
import sqlparse
from sqlalchemy import create_engine
from sqlparse.sql import Identifier, IdentifierList, Parenthesis, Token, Where

from sqlpt.service import (get_truth_table_result, get_field_alias, get_field_expression,
                           get_join_kind, is_equivalent,
                           is_join, remove_whitespace)


class SqlStr(str):
    def parse_fields(self):
        regex = r'(?P<expression>\w+(?:\([^\)]*\))?|\([^\)]*\))[ ]?(?P<alias>\w*)'

        pattern = re.compile(regex)

        fields = []

        for match in re.finditer(pattern, self):
            field = Field(match.group('expression'), match.group('alias'))
            fields.append(field)

        return fields

    def parse_field(self):
        regex = r'(\w*)?(\(.*\))?[ ]*(\w*)'
        match_obj = re.match(regex, self, re.M|re.I)

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

    def __init__(self, *args):
        if len(args) == 1:
            if type(args[0]) == str:
                field_str = args[0]
                expression = get_field_expression(field_str)
                alias = get_field_alias(field_str)

            elif type(args[0]) == list:
                # TODO Untested
                expression = args[0][0]
                alias = args[0][1]

        elif len(args) == 2:
            expression = args[0]
            alias = args[1]

        self.expression = expression
        self.alias = alias

    def __hash__(self):
        return hash(str(self))

    def __str__(self):
        alias = f' {self.alias}' if self.alias else ''
        description = f'{self.expression}{alias}'

        return description


@dataclass
class SelectClause:
    fields: list

    def __init__(self, *args):
        if len(args) == 1:
            if type(args[0]) == str:
                sql_str = args[0]

                sql_tokens = remove_whitespace(sqlparse.parse(sql_str)[0].tokens)

                token_list = []

                for sql_token in sql_tokens:
                    if type(sql_token) == Token:
                        if sql_token.value.lower() == 'from':
                            break

                    elif type(sql_token) == Where:
                        break

                    token_list.append(sql_token)

            elif type(args[0]) == list:
                token_list = args[0]

            # Remove "select" token
            if str(token_list[0]) == 'select':
                token_list.pop(0)

            if type(token_list[0]) == list:
                field_token_list = token_list[0]
            elif type(token_list[0]) == IdentifierList:
                field_token_list = token_list[0]
            else:
                field_token_list = [token_list[0]]

            fields = self.parse_fields(field_token_list)

        # TODO Figure out how to handle a list of Field objects passed in as the single positional argument
        self.fields = fields

    def __hash__(self):
        return hash(str(self))

    def __str__(self):
        select_clause_str = f"select {', '.join(self.field_strs)}"

        return select_clause_str

    def parse_fields(self, field_token_list):
        fields = []

        regex = r'(?P<expression>[\w\*]+(?:\([^\)]*\))?|\([^\)]*\))[ ]?(?P<alias>\w*)'
        pattern = re.compile(regex)

        # FUTR: Chain the "remove" functionality
        for identifier in remove_whitespace(field_token_list, (';', ',')):
            match_obj = re.match(pattern, str(identifier))
            field_args = (match_obj.group('expression'), match_obj.group('alias'))
            field = Field(*field_args)
            fields.append(field)

        return fields

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


@dataclass
class ExpressionClause:
    leading_word: str
    expression: str

    def __init__(self, *args):
        if len(args) == 1:
            if type(args[0]) == str:
                sql_str = args[0]

                sql_tokens = remove_whitespace(sqlparse.parse(sql_str)[0].tokens)

                expression_clause_token_list = []

                start_appending = False

                for sql_token in sql_tokens:
                    if type(sql_token) == Token:
                        if sql_token.value.lower() == 'on':
                            start_appending = True

                    if type(sql_token) == Where:
                        start_appending = True

                    if start_appending:
                        expression_clause_token_list.append(sql_token)

            elif type(args[0]) == list:
                expression_clause_token_list = args[0]

            expression = ''

            for sql_token in expression_clause_token_list:
                # TODO Trim in beforehand to not need parsing anywhere else
                trimmed_sql_token = ' '.join(str(sql_token).split())
                expression += f'{trimmed_sql_token} '

            expression = expression[:-1]

            full_expression_words = expression.split(' ')

            leading_word = full_expression_words.pop(0)
            expression = ' '.join(full_expression_words)

        elif len(args) == 2:
            leading_word = args[0]
            expression = args[1]

        self.leading_word = leading_word
        self.expression = expression

    def __hash__(self):
        return hash(str(self))

    def __bool__(self):
        if not self.leading_word and not self.expression:
            return False

        return True

    def __str__(self):
        string = f'{self.leading_word} {self.expression}' if self else ''

        return string

    def is_equivalent_to(self, other):
        equivalent = (get_truth_table_result(self.expression) ==
                      get_truth_table_result(other.expression))

        return equivalent

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

    def __init__(self, *args):
        if len(args) == 1:
            if type(args[0]) == str:
                # TODO: Implement this
                pass

            if type(args[0]) == list:
                # TODO: Implement this
                pass

        elif len(args) == 3:
            kind = args[0]
            dataset = args[1]
            on_clause = args[2]

        self.kind = kind
        self.dataset = dataset
        self.on_clause = on_clause

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
    # TODO: Add another attribute for sql_str (orig?)
    from_table: Table
    joins: list

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

    # TODO: Allow kwargs; args xor kwargs; apply to all __init__ methods
    def __init__(self, *args):
        if len(args) == 1:
            if type(args[0]) == str:
                sql_str = args[0]

                sql_tokens = remove_whitespace(sqlparse.parse(sql_str)[0].tokens)

                from_clause_token_list = []

                start_appending = False

                for sql_token in sql_tokens:
                    if type(sql_token) == Token:
                        if sql_token.value.lower() == 'from':
                            start_appending = True

                    elif type(sql_token) == Where:
                        break

                    if start_appending:
                        from_clause_token_list.append(sql_token)

            elif type(args[0]) == list:
                from_clause_token_list = args[0]

            from_table, joins = self._parse_from_clause(from_clause_token_list)

        elif len(args) == 2:
            from_table = args[0]
            joins = args[1]

        self.from_table = from_table
        self.joins = joins

    def __hash__(self):
        return hash(str(self))

    def __str__(self):
        from_clause_str = ''

        if self.from_table:
            from_clause_str = f'from {self.from_table}'

            for join in self.joins:
                from_clause_str += f'{join}'

        return from_clause_str

    def _parse_from_clause(self, token_list):
        from_table = None
        joins = []

        if token_list:
            # Remove 'from' keyword for now
            token_list.pop(0)

            # Get from_table
            from_table_name = str(token_list.pop(0))
            # May need to handle subquery; it will be a Parenthesis object
            from_table = Table(from_table_name)

            # Construct joins
            kind = None
            dataset = None
            on_tokens = []

            for item in token_list:
                # Parse join item
                if is_join(item):
                    # Create join object with previously populated values
                    # if applicable, and clear out values for a next one
                    if kind and dataset and on_tokens:
                        join_kind = deepcopy(str(kind))
                        join_dataset = deepcopy(str(dataset))
                        join_on_clause = OnClause(on_tokens)

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
                on_clause = OnClause(on_tokens)

                join = Join(kind, dataset, on_clause)
                joins.append(join)

        return from_table, joins

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

    def first_join_dataset(self):
        first_join = self.joins[0]

        if first_join.kind == 'join':
            first_join_dataset = first_join.dataset
        else:
            first_join_dataset = None

        return first_join_dataset


# TODO Remove this maybe? No longer needed?
@dataclass
class Comparison:
    left_term: str
    operator: str
    right_term: str

    def __init__(self, *args):
        if len(args) == 1:
            if type(args[0]) == str:
                comparison_str = args[0]
                statement = sqlparse.parse(comparison_str)
                sqlparse_comparison = statement[0].tokens[0]
                # TODO Remove whitespace in before this point
                comparison_tokens = remove_whitespace(sqlparse_comparison.tokens)

            elif type(args[0]) == list:
                # TODO Untested
                comparison_tokens = args[0]

            elements = [comparison_token.value
                        for comparison_token in comparison_tokens]

        elif len(args) == 3:
            elements = args

        self.left_term = elements[0]
        self.operator = elements[1]
        self.right_term = elements[2]

    def __hash__(self):
        return hash(str(self))

    def __str__(self):
        string = f'{self.left_term} {self.operator} {self.right_term}'

        return string

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


class WhereClause(ExpressionClause):
    pass


@dataclass
class Query(DataSet):
    select_clause: SelectClause
    from_clause: FromClause
    where_clause: WhereClause

    def __init__(self, *args):
        # TODO: What if a sole select clause object is passed in?
        if len(args) == 1:
            if type(args[0]) == str:
                sql_str = args[0]

                sql_tokens = remove_whitespace(sqlparse.parse(sql_str)[0].tokens)

                select_clause_token_list = []
                from_clause_token_list = []
                where_clause_token_list = []

                clause_token_list = select_clause_token_list

                for sql_token in sql_tokens:
                    if type(sql_token).__name__ == 'Token':
                        if sql_token.value.lower() == 'from':
                            clause_token_list = from_clause_token_list

                    elif type(sql_token).__name__ == 'Where':
                        clause_token_list = where_clause_token_list

                    clause_token_list.append(sql_token)

                clauses_tuple = (
                    select_clause_token_list,
                    from_clause_token_list,
                    where_clause_token_list,
                )

                # TODO Make sure to get the correct where clause when parsing sql with
                #     more than one
                # TODO Figure out why leaving self.expression as '' results in:
                #     SyntaxWarning: null string passed to Literal; use Empty() instead
                # TODO Make this into a function and find a better place or it
                select_clause = SelectClause(clauses_tuple[0])
                from_clause = FromClause(clauses_tuple[1])
                where_clause = WhereClause(clauses_tuple[2])

            elif type(args[0]) == list:
                select_clause = args[0][0]
                from_clause = args[0][1]
                where_clause = args[0][2]

        elif len(args) == 2:
            select_clause = args[0]
            from_clause = args[1]
            where_clause = None

        elif len(args) == 3:
            select_clause = args[0]
            from_clause = args[1]
            where_clause = args[2]

        self.select_clause = select_clause
        self.from_clause = from_clause
        self.where_clause = where_clause

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        query_egual = False

        if isinstance(other, Query):
            select_clauses_equal = self.select_clause == other.select_clause
            from_clauses_equal = self._optional_clause_equal(other, 'from')
            where_clauses_equal = self._optional_clause_equal(other, 'where')

            query_egual = (
                select_clauses_equal and
                from_clauses_equal and
                where_clauses_equal)

        return query_egual

    def __str__(self):
        string = str(self.select_clause)

        if self.from_clause:
            string += f' {self.from_clause}'

        if hasattr(self, 'where_clause'):
            if self.where_clause:
                string += f' {self.where_clause}'

        return string

    def _optional_clause_equal(self, other, kind):
        clauses_equal = False

        self_has_clause = hasattr(self, f'{kind}_clause')
        other_has_clause = hasattr(other, f'{kind}_clause')

        if self_has_clause and other_has_clause:
            clauses_equal = (getattr(self, f'{kind}_clause') ==
                             getattr(other, f'{kind}_clause'))
        elif self_has_clause and not other_has_clause:
            clauses_equal = False
        elif not self_has_clause and other_has_clause:
            clauses_equal = False
        else:
            clauses_equal = True

        return clauses_equal

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
