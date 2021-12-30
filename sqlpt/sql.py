""" docstring tbd """

import re
from copy import deepcopy
from dataclasses import dataclass
from dataclasses import field as dataclass_field

import pandas as pd
import sqlparse
from sqlalchemy import create_engine, exc, inspect
from sqlparse.sql import Comparison as SqlParseComparison
from sqlparse.sql import Identifier, IdentifierList, Parenthesis, Token, Where

from sqlpt.service import (
    get_field_alias, get_field_expression, get_join_kind,
    get_truth_table_result, is_equivalent, is_join, remove_whitespace)


class SqlStr(str):
    """ docstring tbd """
    def parse_fields(self):
        """ docstring tbd """
        sql_str = f'select {self}' if self[:6] != 'select' else self

        token_list = parse_select_clause(sql_str)
        fields = parse_fields_from_token_list(token_list)

        return fields

    def parse_field(self):
        """ docstring tbd """
        regex = r'(\w*)?(\(.*\))?[ ]*(\w*)'
        match_obj = re.match(regex, self, re.M | re.I)

        expression_part_1 = match_obj.group(1)
        expression_part_2 = match_obj.group(2)

        if expression_part_2:
            expression = f'{expression_part_1}{expression_part_2}'
        else:
            expression = expression_part_1

        alias = match_obj.group(3)
        field = Field(expression, alias)

        return field


class QueryResult(list):
    """ docstring tbd """
    def count(self):
        return len(self)


def get_dataset(token):
    """ docstring tbd """
    dataset = None

    if type(token) == Parenthesis:
        sql_str = str(token)[1:-1]
        dataset = Query(sql_str)

    else:
        dataset = Table(str(token))

    return dataset


@dataclass
class DataSet:
    """ docstring tbd """

    def rows_unique(self, field_names):
        """ docstring tbd """
        fields = [Field(field_name) for field_name in field_names]
        select_clause = SelectClause(fields)
        select_clause.add_field('count(*)')

        from_clause = FromClause(self)

        group_by_clause = GroupByClause(field_names)
        having_clause = HavingClause('having count(*) > 1')

        query = Query(select_clause=select_clause,
                      from_clause=from_clause,
                      group_by_clause=group_by_clause,
                      having_clause=having_clause)

        unique = not query.rows_exist()

        return unique


@dataclass
class Table(DataSet):
    """ docstring tbd """
    name: str

    def __hash__(self):
        return hash(str(self))

    def __str__(self):
        return self.name

    def count(self):
        """ docstring tbd """
        query = Query(f'select rowid from {self.name}')
        row_count = query.run().count()

        return row_count

    def get_columns(self):
        """ docstring tbd """
        engine = create_engine('sqlite:///sqlpt/college.db')
        insp = inspect(engine)
        columns = insp.get_columns(self.name)

        return columns

    def is_equivalent_to(self, other):
        """ docstring tbd """
        equivalent = False

        if isinstance(other, self.__class__):
            equivalent = self.name == other.name

        return equivalent


def parse_select_clause(sql_str):
    """ docstring tbd """
    sql_tokens = remove_whitespace(sqlparse.parse(sql_str)[0].tokens)

    token_list = []

    if str(sql_tokens[0]).lower() == 'select':
        for sql_token in sql_tokens:
            if type(sql_token) == Token:
                if sql_token.value.lower() == 'from':
                    break

            elif type(sql_token) == Where:
                break

            token_list.append(sql_token)

    return token_list


def parse_fields_from_token_list(field_token_list):
    """ docstring tbd """
    regex = (
        r'(?P<expression>\'?[\w\*]+\'?(?:\([^\)]*\))?|\([^\)]*\))[ ]?(?P<alias>\w*)')  # noqa

    # old version
    # r'(?P<expression>\w+(?:\([^\)]*\))?|\([^\)]*\))[ ]?(?P<alias>\w*)')

    pattern = re.compile(regex)

    fields = []

    # FUTURE: Chain the "remove" functionality
    for identifier in remove_whitespace(field_token_list, (';', ',')):
        match_obj = re.match(pattern, str(identifier))
        expression = match_obj.group('expression')
        alias = match_obj.group('alias')

        field = Field(expression, alias)
        fields.append(field)

    return fields


def convert_token_list_to_fields(token_list):
    """ docstring tbd """
    # Remove "select" token
    fields = []

    if token_list:
        if str(token_list[0]) == 'select':
            token_list.pop(0)

        if type(token_list[0]) == list:
            field_token_list = token_list[0]
        elif type(token_list[0]) == IdentifierList:
            field_token_list = token_list[0]
        else:
            field_token_list = [token_list[0]]

        fields = parse_fields_from_token_list(field_token_list)

    return fields


@dataclass
class SelectClause:
    """ docstring tbd """
    fields: list

    def __init__(self, *args):
        if len(args) == 1:
            if type(args[0]) == str:
                sql_str = args[0]
                token_list = parse_select_clause(sql_str)
                fields = convert_token_list_to_fields(token_list)

            elif type(args[0]) == list:
                if args[0]:
                    if type(args[0][0]) == Token:
                        token_list = args[0]
                        fields = convert_token_list_to_fields(token_list)

                    elif type(args[0][0]) == Field:
                        fields = args[0]

                    else:
                        if type(args[0][1]) == list:
                            field_strs = args[0][1]
                        else:
                            field_strs = args[0][1:]

                        sql_str = ', '.join(field_strs)
                        sql_str = f'select {sql_str}'
                        token_list = parse_select_clause(sql_str)
                        fields = convert_token_list_to_fields(token_list)
                else:
                    fields = []

        self.fields = fields

    def __hash__(self):
        return hash(str(self))

    def __str__(self):
        select_clause_str = f"select {', '.join(self.field_strs)}"

        return select_clause_str

    @property
    def field_strs(self):
        """ docstring tbd """
        field_strs = [str(field) for field in self.fields]

        return field_strs

    def _get_field(self, *args):
        """ docstring tbd """
        if type(args[0]) == str:
            field = Field(args[0])
        elif type(args[0]) == Field:
            field = args[0]
        else:
            field = Field(*args)

        return field

    def add_field(self, *args):
        """ docstring tbd """
        field = self._get_field(*args)

        self.fields.append(field)

    def remove_field(self, *args):
        """ docstring tbd """
        field = self._get_field(*args)

        self.fields.remove(field)

    def is_equivalent_to(self, other):
        """ docstring tbd """
        equivalent = False

        if isinstance(other, self.__class__):
            equivalent = set(self.fields) == set(other.fields)

        return equivalent

    def fuse(self, select_clause):
        """ docstring tbd """


def get_expression_clause_parts(token_list):
    expression = ''

    for sql_token in token_list:
        trimmed_sql_token = ' '.join(str(sql_token).split())
        expression += f'{trimmed_sql_token} '

    expression = expression[:-1]

    full_expression_words = expression.split(' ')

    if full_expression_words[0] in ('on', 'where', 'having', 'set'):
        full_expression_words.pop(0)

    expression = Expression(' '.join(full_expression_words))

    return expression


@dataclass
class Expression:
    comparisons: list

    def __init__(self, *args):
        if len(args) == 1:
            if type(args[0]) == str:
                comparisons = []

                if args[0]:
                    statement = sqlparse.parse(args[0])

                    expression_tokens = (remove_whitespace(statement[0].tokens))
                    comparison_token_list = []
                    comparison_token_lists = []

                    for token in expression_tokens:
                        if type(token) != SqlParseComparison:
                            comparison_token_list.append(token)

                        elif type(token) == SqlParseComparison:
                            comparison_token_list.append(token)
                            comparison_token_lists.append(comparison_token_list)  # deepcopy?
                            comparison_token_list = []

                    for comparison_token_list in comparison_token_lists:
                        comparison = Comparison(comparison_token_list)
                        comparisons.append(comparison)

        self.comparisons = comparisons

    def __str__(self):
        string = ''

        for comparison in self.comparisons:
            string += f'{str(comparison)} '

        string = string[:-1]

        return string


@dataclass
class ExpressionClause:
    """ docstring tbd """
    leading_word: str = dataclass_field(repr=False)
    expression: Expression

    def __init__(self, *args):
        if len(args) == 1:
            if type(args[0]) == str:
                sql_str = args[0]
                expression_clause_token_list = (
                    self.parse_expression_clause(sql_str))

                expression = get_expression_clause_parts(
                    expression_clause_token_list)

            elif type(args[0]) == list:
                expression_clause_token_list = args[0]

                expression = get_expression_clause_parts(
                    expression_clause_token_list)

        elif len(args) == 2:
            expression = args[1]

        self.expression = expression

    def __hash__(self):
        return hash(str(self))

    def __bool__(self):
        if self.expression.comparisons:
            return True

        return False

    def __str__(self):
        string = f'{self.leading_word} {self.expression}' if self else ''

        return string

    def parse_expression_clause(self, sql_str):
        """ docstring tbd """
        raise NotImplementedError

    def is_equivalent_to(self, other):
        """ docstring tbd """
        equivalent = (get_truth_table_result(str(self.expression)) ==
                      get_truth_table_result(str(other.expression)))

        return equivalent

    def parameterize(self, parameter_fields):
        """ docstring tbd

        for comparison in self.comparisons:
            left_term = comparison.left_term

            if left_term in parameter_fields:
                comparison.right_term = f':{left_term}'

        return self
        """

        # TODO Start with matching the comparison, kind of like this:
        #      expr_w_parens = re.sub(r'(\w+\s*=\s*\w+)', r'(\1)', expr)

        raise Exception(f'Needs implementation: {parameter_fields}')


def parse_on_clause(sql_str):
    """ docstring tbd """
    sql_tokens = (
        remove_whitespace(sqlparse.parse(sql_str)[0].tokens))

    on_clause_token_list = []

    for sql_token in sql_tokens:
        on_clause_token_list.append(sql_token)

    return on_clause_token_list


class OnClause(ExpressionClause):
    """ docstring tbd """
    def __init__(self, *args):
        super().__init__(*args)

        self.leading_word = 'on'

    def parse_expression_clause(self, sql_str):
        """ docstring tbd """
        token_list = parse_on_clause(sql_str)

        return token_list


@dataclass
class Join:
    """ docstring tbd """
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
        if isinstance(self.dataset, Query):
            dataset_str = f'({self.dataset})'
        else:
            dataset_str = self.dataset

        join_str = f' {self.kind_str} {dataset_str} {self.on_clause}'

        return join_str

    @property
    def kind_str(self):
        """ docstring tbd """
        join_prefix = 'join' if self.kind == 'inner' else f'{self.kind} join'

        return join_prefix

    def is_equivalent_to(self, other):
        """ docstring tbd """
        equivalent = (self.kind == other.kind
                      and
                      is_equivalent([self.dataset], [other.dataset])
                      and
                      self.on_clause.is_equivalent_to(other.on_clause))

        return equivalent


def parse_from_clause(sql_str):
    """ docstring tbd """
    sql_tokens = (
        remove_whitespace(sqlparse.parse(sql_str)[0].tokens))

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

    return from_clause_token_list


@dataclass
class FromClause:
    """ docstring tbd """
    from_dataset: DataSet
    joins: list

    # TODO: Allow kwargs; args xor kwargs; apply to all __init__ methods
    def __init__(self, *args):
        if len(args) == 1:
            if type(args[0]) == str:
                sql_str = args[0]
                from_clause_token_list = parse_from_clause(sql_str)

                from_dataset, joins = self._parse_from_clause(
                    from_clause_token_list)

            elif type(args[0]) == list:
                from_clause_token_list = args[0]

                from_dataset, joins = self._parse_from_clause(
                    from_clause_token_list)

            elif isinstance(args[0], DataSet):
                from_dataset = args[0]
                joins = []

        elif len(args) == 2:
            from_dataset = args[0]
            joins = args[1]

        self.from_dataset = from_dataset
        self.joins = joins

    def __hash__(self):
        return hash(str(self))

    def __bool__(self):
        if self.from_dataset:
            return True

        return False

    def __str__(self):
        from_clause_str = ''

        if self.from_dataset:
            if type(self.from_dataset) == Query:
                dataset_str = f'({self.from_dataset})'

            else:
                dataset_str = str(self.from_dataset)

            from_clause_str = f'from {dataset_str}'

            for join in self.joins:
                from_clause_str += f'{join}'

        return from_clause_str

    # TODO: Reconcile this and parse_from_clause
    def _parse_from_clause(self, token_list):
        """ docstring tbd """
        from_dataset = None
        joins = []

        if token_list:
            # Remove 'from' keyword for now
            token_list.pop(0)

            # Get from_dataset
            token = token_list.pop(0)
            from_dataset = get_dataset(token)

            # Construct joins
            kind = None
            dataset = None
            on_tokens = []

            for token in token_list:
                # Parse join token
                if is_join(token):
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

                    kind = get_join_kind(token)

                    continue

                # Parse dataset token
                if type(token) in (Identifier, Parenthesis):
                    dataset = get_dataset(token)

                    continue

                # Parse comparison token
                on_tokens.append(token)

            # Create the last join
            if kind and dataset and on_tokens:
                on_clause = OnClause(on_tokens)

                join = Join(kind, dataset, on_clause)
                joins.append(join)

        return from_dataset, joins

    def is_equivalent_to(self, other):
        """ docstring tbd """
        equivalent = False

        if isinstance(other, self.__class__):
            # TODO: Allow for equivalence if tables and comparisons are out of
            # order

            equivalent = (self.from_dataset == other.from_dataset
                          or
                          (self.from_dataset == other.first_join_dataset()
                           and
                           other.from_dataset == self.first_join_dataset()))

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
        """ docstring tbd """
        first_join = self.joins[0]

        if first_join.kind == 'join':
            first_join_dataset = first_join.dataset
        else:
            first_join_dataset = None

        return first_join_dataset

    def remove_join(self, join):
        """ docstring tbd """
        self.joins.remove(join)


@dataclass
class Comparison:
    """ docstring tbd """
    bool_operator: str = dataclass_field(repr=False)
    bool_sign: str = dataclass_field(repr=False)
    left_term: str
    operator: str
    right_term: str

    def __init__(self, *args):
        if len(args) == 1:
            if type(args[0]) == str:
                comparison_str = args[0]
                statement = sqlparse.parse(comparison_str)
                sqlparse_comparison = statement[0].tokens[0]
                comparison_tokens = (
                    remove_whitespace(sqlparse_comparison.tokens))

                elements = [comparison_token.value
                            for comparison_token in comparison_tokens]

            elif type(args[0]) == list:
                # TODO Untested
                sqlparse_comparison = args[0][0]

                elements = []

                for sqlparse_comparison in args[0]:
                    if type(sqlparse_comparison) != SqlParseComparison:
                        elements.append(sqlparse_comparison.value)

                    elif type(sqlparse_comparison) == SqlParseComparison:
                        comparison_tokens = (
                            remove_whitespace(sqlparse_comparison.tokens))
                        
                        els = [comparison_token.value
                               for comparison_token in comparison_tokens]

                        elements.extend(els)

            if elements[0] in ('and', 'or'):
                bool_operator = elements.pop(0)
            elif elements[0] in ('not'):
                bool_sign = elements.pop(0)
            else:
                bool_operator = ''

            if elements[0] == 'not':
                bool_sign = elements.pop(0)
            else:
                bool_sign = ''

            left_term = elements[0]
            operator = elements[1]
            right_term = elements[2]

        self.bool_operator = bool_operator
        self.bool_sign = bool_sign
        self.left_term = left_term
        self.operator = operator
        self.right_term = right_term

    def __hash__(self):
        return hash(str(self))

    def __str__(self):
        string = ''

        if self.bool_operator:
            string += f'{self.bool_operator} '

        if self.bool_sign:
            string += f'{self.bool_sign} '

        string += f'{self.left_term} '
        string += f'{self.operator} '
        string += f'{self.right_term}'

        return string

    def is_equivalent_to(self, other):
        """ docstring tbd """
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


def parse_where_clause(sql_str):
    """ docstring tbd """
    sql_tokens = (
        remove_whitespace(sqlparse.parse(sql_str)[0].tokens))

    where_clause_token_list = []

    start_appending = False

    for sql_token in sql_tokens:
        if type(sql_token) == Token:
            if sql_token.value.lower() == 'from':
                continue

        elif type(sql_token) == Where:
            start_appending = True

        if start_appending:
            where_clause_token_list.append(sql_token)

    return where_clause_token_list


class WhereClause(ExpressionClause):
    """ docstring tbd """
    def __init__(self, *args):
        super().__init__(*args)

        self.leading_word = 'where'

    def parse_expression_clause(self, sql_str):
        """ docstring tbd """
        token_list = parse_where_clause(sql_str)

        return token_list


class GroupByClause:
    field_names: list

    def __init__(self, *args):
        if len(args) == 1:
            if type(args[0]) == list:
                # TODO: Also allow list of Field objects
                field_names = args[0]

        self.field_names = field_names

    def __str__(self):
        if self.field_names:
            string = 'group by '

            for field_name in self.field_names:
                string += f'{field_name}, '

            # Remove trailing comma and space
            string = string[:-2]

        return string


def parse_having_clause(sql_str):
    """ docstring tbd """
    sql_tokens = (
        remove_whitespace(sqlparse.parse(sql_str)[0].tokens))

    having_clause_token_list = []

    for sql_token in sql_tokens:
        having_clause_token_list.append(sql_token)

    return having_clause_token_list


class HavingClause(ExpressionClause):
    """ docstring tbd """
    def __init__(self, *args):
        super().__init__(*args)

        self.leading_word = 'having'

    def parse_expression_clause(self, sql_str):
        """ docstring tbd """
        token_list = parse_having_clause(sql_str)

        return token_list


def delete_node(query, coordinates):
    """ docstring tbd """
    node = query

    for coordinate in coordinates:
        for component in coordinate:
            if type(component) == str:
                node = getattr(node, component)

            else:
                # Delete the nth node, not just the part of the node, which
                # would break the query (hence the `break`)
                node.pop(component)
                break

    return query


def parameterize_node(query, coordinates):
    """ docstring tbd """
    node = query
    leaf_node = None

    for coordinate in coordinates:
        for component in coordinate:
            if type(component) == str:
                node = getattr(node, component)

            else:
                node = node[component]
                leaf_node = node

    # Assuming (I know...) that leaf_node is an instance of Comparison
    # To parameterize a comparison, use a standard approach where the bind
    # parameter is the right_term, so if the invalid column is the left_term,
    # swap them first and then give the right_term a standard bind-parameter
    # name of :[left_term] (replacing . with _)
    if leaf_node:
        if component == 'left_term':
            leaf_node.left_term = leaf_node.right_term

        leaf_node.right_term = f":{leaf_node.left_term.replace('.', '_')}"

    return query


@dataclass
class Query(DataSet):
    """ docstring tbd """
    sql_str: SqlStr = dataclass_field(repr=False)
    select_clause: SelectClause
    from_clause: FromClause
    where_clause: WhereClause
    group_by_clause: GroupByClause
    having_clause: HavingClause

    def __init__(self, *args, **kwargs):
        if len(args) == 1:
            if type(args[0]) == str:
                # TODO: Distinguish between s_str and sql_str everywhere
                #       s_str being a snippet? and sql_str a full query sql
                s_str = args[0]
                select_clause = SelectClause(s_str)
                from_clause = FromClause(s_str) or None
                where_clause = WhereClause(s_str) or None
                group_by_clause = None
                having_clause = None

            elif type(args[0]) == list:
                s_str = ''
                # TODO: Accommodate for missing from_clause and where_clause
                select_clause = args[0][0]
                from_clause = args[0][1]
                where_clause = args[0][2]
                group_by_clause = None
                having_clause = None

        elif len(args) == 2:
            s_str = ''
            select_clause = args[0]
            from_clause = args[1]
            where_clause = None
            group_by_clause = None
            having_clause = None

        elif len(args) == 3:
            s_str = ''
            select_clause = args[0]
            from_clause = args[1]
            where_clause = args[2]
            group_by_clause = None
            having_clause = None

        else:
            s_str = ''
            select_clause = kwargs.get('select_clause')
            from_clause = kwargs.get('from_clause')
            where_clause = kwargs.get('where_clause')
            group_by_clause = kwargs.get('group_by_clause')
            having_clause = kwargs.get('having_clause')

        self.sql_str = SqlStr(s_str)
        self.select_clause = select_clause
        self.from_clause = from_clause
        self.where_clause = where_clause
        self.group_by_clause = group_by_clause
        self.having_clause = having_clause

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        query_equal = False

        if isinstance(other, Query):
            select_clauses_equal = self.select_clause == other.select_clause
            from_clauses_equal = self._optional_clause_equal(other, 'from')
            where_clauses_equal = self._optional_clause_equal(other, 'where')

            query_equal = (
                select_clauses_equal and
                from_clauses_equal and
                where_clauses_equal)

        return query_equal

    def __str__(self):
        string = str(self.select_clause)

        if self.from_clause:
            if str(self.from_clause):
                string += f' {self.from_clause}'

        if hasattr(self, 'where_clause'):
            if self.where_clause:
                string += f' {self.where_clause}'

        if hasattr(self, 'group_by_clause'):
            if self.group_by_clause:
                string += f' {self.group_by_clause}'

        if hasattr(self, 'having_clause'):
            if self.having_clause:
                string += f' {self.having_clause}'

        return string

    def _optional_clause_equal(self, other, kind):
        """ docstring tbd """
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
        """ docstring tbd """
        db_conn = create_engine('sqlite:///sqlpt/college.db')

        return db_conn

    @property
    def dataframe(self):
        """ docstring tbd """
        data_frame = pd.read_sql_query(self.__str__(), self.db_conn)

        return data_frame

    def locate_column(self, s_str):
        """ docstring tbd """
        locations = []

        # TODO: Move this to be a method of the SelectClause?
        for i, field in enumerate(self.select_clause.fields):
            if s_str in field.expression:
                locations.append(('select_clause', 'fields', i))

            # TODO: Improve this check; should a field always have a query attribute even if None? Now, I'm leaning toward no
            if hasattr(field, 'query'):
                if type(field.query) == Query:
                    locations.extend(field.query.locate_column(s_str))

        # TODO: Move this to be a method of the FromClause?
        for i, join in enumerate(self.from_clause.joins):
            # TODO: Move this to be a method of the OnClause? (ExpressionClause)
            for j, comparison in enumerate(join.on_clause.expression.comparisons):
                if s_str in comparison.left_term:
                    locations.append(('from_clause', 'joins', i, 'on_clause', 'expression', 'comparisons', j, 'left_term'))
                elif s_str in comparison.right_term:
                    locations.append(('from_clause', 'joins', i, 'on_clause', 'expression', 'comparisons', j, 'right_term'))

        # TODO: Move this to be a method of the WhereClause? (ExpressionClause)
        for i, comparison in enumerate(self.where_clause.expression.comparisons):
            if s_str in comparison.left_term:
                locations.append(('where_clause', 'expression', 'comparisons', i, 'left_term'))
            elif s_str in comparison.right_term:
                locations.append(('where_clause', 'expression', 'comparisons', i, 'right_term'))

        return locations

    def delete_node(self, coordinates):
        """ docstring tbd """
        remaining_query = delete_node(self, coordinates)

        return remaining_query

    def locate_invalid_columns(self):
        """ docstring tbd """
        invalid_column_coordinates = []

        with self.db_conn.connect() as db_conn:
            try:
                db_conn.execute(str(self))

            except exc.OperationalError as e:
                if 'no such column' in str(e):
                    error_msg = str(e).split('\n')[0]
                    invalid_column_name = error_msg.split(': ')[1]
                    invalid_column_coordinates = self.locate_column(
                        invalid_column_name)

        return invalid_column_coordinates

    def crop(self):
        """ docstring tbd """
        invalid_column_coordinates = self.locate_invalid_columns()
        cropped_query = self.delete_node(invalid_column_coordinates)

        return cropped_query

    def parameterize_node(self, coordinates):
        """ docstring tbd """
        parameterized_query = parameterize_node(self, coordinates)

        return parameterized_query

    # TODO: Not related to this location of code, but: change
    #       Comparison.bool_operator to Comparison.bool_conjunction
    def parameterize(self):
        """ docstring tbd """
        # self.where_clause.parameterize(parameter_fields)
        invalid_column_coordinates = self.locate_invalid_columns()
        parameterized_query = self.parameterize_node(
            invalid_column_coordinates)

        return parameterized_query

    def run(self, **kwargs):
        """ docstring tbd """
        rows = []

        with self.db_conn.connect() as db_conn:
            rows = db_conn.execute(str(self), **kwargs)
            row_dicts = QueryResult()

            for row in rows:
                row_dict = dict(row._mapping.items())
                row_dicts.append(row_dict)

        return row_dicts

    def count(self, **kwargs):
        """ docstring tbd """
        return len(self.run(**kwargs))

    def counts(self):
        """ docstring tbd """
        counts_dict = {}
        query_count = self.run().count()
        counts_dict['query'] = query_count

        from_dataset = self.from_clause.from_dataset
        counts_dict[from_dataset.name] = from_dataset.count()

        for join in self.from_clause.joins:
            counts_dict[join.dataset.name] = join.dataset.count()

        return counts_dict

    def rows_exist(self, **kwargs):
        row_count = self.count(**kwargs)

        rows_exist_bool = True if row_count != 0 else False

        return rows_exist_bool

    def scalarize(self):
        """ docstring tbd """
        joins_to_remove = []

        for join in self.from_clause.joins:
            if join.kind == 'left':
                dataset_columns = join.dataset.get_columns()
                column_names = [
                    col_dict['name'] for col_dict in dataset_columns]

                for field in self.select_clause.fields:
                    if field.expression in column_names:
                        self.select_clause.remove_field(field)

                        subquery_select_clause = SelectClause([field])
                        subquery_from_clause = FromClause(join.dataset)
                        # TODO: Remove 'where' when able to construct
                        #       WhereClause with an Expression Class (tbd)
                        #       instance
                        subquery_where_clause = WhereClause('where', join.on_clause.expression)
                        subquery = Query(
                            subquery_select_clause, subquery_from_clause,
                            subquery_where_clause)

                        alias = field.alias or field.expression
                        subquery_field = Field(subquery, alias)
                        # TODO: Substitute this with add_subquery_field?
                        self.select_clause.add_field(subquery_field)

                        joins_to_remove.append(join)
        
        for join_to_remove in joins_to_remove:
            self.from_clause.remove_join(join_to_remove)

        scalarized_query = Query(
            self.select_clause, self.from_clause, self.where_clause)

        return scalarized_query

    def is_leaf(self):
        """ docstring tbd """
        contains_subqueries = False

        for field in self.select_clause.fields:
            if field.query:
                contains_subqueries = True
                break

        if not contains_subqueries:
            if type(self.from_clause.from_dataset) == Query:
                contains_subqueries = True

        if not contains_subqueries:
            # TODO: Check if a subquery lives in the join's on_clause
            for join in self.from_clause.joins:
                if type(join.dataset) == Query:
                    contains_subqueries = True
                    break

        # TODO: Check if a subquery lives in the where_clause

        return not contains_subqueries

    def fuse(self, query):
        """ docstring tbd """
        # FUTURE: Figure out how to fuse from clauses
        if self.from_clause == query.from_clause:
            self.select_clause.fuse(query.select_clause)
            self.where_clause.fuse(query.where_clause)

        return self

    def bind_params(self, **kwargs):
        """ docstring tbd """
        for key, value in kwargs.items():
            bound_sql_str = self.__str__().replace(f':{key}', str(value))
            self.__init__(bound_sql_str)

        return self

    def format_sql(self):
        """ docstring tbd """
        formatted_sql = sqlparse.format(self.__str__())

        return formatted_sql

    def describe(self):
        """ docstring tbd """
        description = self.dataframe.describe()

        return description

    def head(self, num_rows=5):
        """ docstring tbd """
        head_data = self.dataframe.head(num_rows)

        return head_data

    def output_sql_file(self, path):
        """ docstring tbd """
        with open(path, 'wt') as sql_file:
            sql_file.write(self.format_sql())

    def output_data_file(self, path):
        """ docstring tbd """
        self.dataframe.to_csv(path, index=False)

    def subquery_str(self):
        """ docstring tbd """
        string = f'({self.__str__()})'

        return string

    def add_subquery_field(self, subquery_str, alias):
        """ docstring tbd """
        self.select_clause.add_field(subquery_str, alias)

    def filter_by_subquery(self, subquery_str, operator, value):
        """ docstring tbd """
        if type(value) == list:
            if operator == '=':
                operator = 'in'

            value = ','.join(f"{item}" for item in value if item)
            value = f'({value})'

        comparison = Comparison(subquery_str, operator, value)

        self.where_clause.add_comparison(comparison)


# TODO: Replace sql_str with s_str where it makes sense
def get_query_from_subquery_str(s_str):
    """ docstring tbd """
    query = Query(s_str[1:-1]) if s_str[:7] == '(select' else None

    return query


@dataclass
class Field:
    """ docstring tbd """
    expression: str
    alias: str
    query: Query = dataclass_field(repr=False)

    def __init__(self, *args):
        if len(args) == 1:
            if type(args[0]) == str:
                field_str = args[0]
                # TODO: Make sure this accommodates subqueries as expressions
                expression = get_field_expression(field_str)
                alias = get_field_alias(field_str)
                query = Query(expression)

            elif type(args[0]) == list:
                # TODO: What is the use case here again?
                expression = args[0][0]
                alias = args[0][1]
                query = Query(expression)

        elif len(args) == 2:
            if type(args[0]) == Query:
                expression = f'({str(args[0])})'
                alias = args[1]
                query = args[0]

            else:
                expression = args[0]
                alias = args[1]
                # query = Query(args[0][1:-1]) if args[0][:7] == '(select' else None
                query = get_query_from_subquery_str(args[0])

        self.expression = expression
        self.alias = alias
        self.query = query

    def __hash__(self):
        return hash(str(self))

    def __str__(self):
        alias = f' {self.alias}' if self.alias else ''
        description = f'{self.expression}{alias}'

        return description


@dataclass
class UpdateClause:
    dataset: DataSet

    # TODO: Allow kwargs; args xor kwargs; apply to all __init__ methods
    def __init__(self, *args):
        dataset = None

        if len(args) == 1:
            sql_str = args[0]

            if type(args[0]) == str:
                sql_parts = sql_str.split()

                leading_word = 'update'

                if len(sql_parts) == 1:
                    dataset = sql_parts[0]

                else:
                    # TODO: Better handle full update-statement sql getting
                    #       passed in
                    dataset = sql_parts[1]

        self.leading_word = leading_word

        self.dataset = dataset

    def __str__(self):
        update_clause_str = f'update {self.dataset}'

        return update_clause_str


def parse_set_clause(sql_str):
    """ docstring tbd """
    sql_tokens = (
        remove_whitespace(sqlparse.parse(sql_str)[0].tokens))

    set_clause_token_list = []

    for sql_token in sql_tokens:
        set_clause_token_list.append(sql_token)

    return set_clause_token_list


@dataclass
class SetClause(ExpressionClause):
    def __init__(self, *args):
        super().__init__(*args)

        self.leading_word = 'set'

    def parse_expression_clause(self, sql_str):
        """ docstring tbd """
        token_list = parse_set_clause(sql_str)

        return token_list


# TODO: Find commonality between this and a Query
@dataclass
class UpdateStatement:
    """ docstring tbd """
    sql_str: SqlStr = dataclass_field(repr=False)
    update_clause: UpdateClause
    set_clause: SetClause
    where_clause: WhereClause

    def __init__(self, *args, **kwargs):
        if len(args) == 1:
            if type(args[0]) == str:
                # TODO: Distinguish between s_str and sql_str everywhere
                #       s_str being a snippet? and sql_str a full query sql
                s_str = args[0]
                update_clause = UpdateClause(s_str)
                set_clause = SetClause(s_str)
                where_clause = WhereClause(s_str) or None

            elif type(args[0]) == list:
                s_str = ''
                # TODO: Accommodate for missing where_clause
                update_clause = args[0][0]
                set_clause = args[0][1]
                where_clause = args[0][2]

        elif len(args) == 2:
            s_str = ''
            update_clause = args[0]
            set_clause = args[1]
            where_clause = None

        elif len(args) == 3:
            s_str = ''
            update_clause = args[0]
            set_clause = args[1]
            where_clause = args[2]

        else:
            s_str = ''
            update_clause = kwargs.get('update_clause')
            set_clause = kwargs.get('set_clause')
            where_clause = kwargs.get('where_clause')

        self.sql_str = SqlStr(s_str)
        self.update_clause = update_clause
        self.set_clause = set_clause
        self.where_clause = where_clause

    def __str__(self):
        string = str(self.update_clause)

        string += f' {self.set_clause}'

        if hasattr(self, 'where_clause'):
            if self.where_clause:
                string += f' {self.where_clause}'

        return string

    def count(self):
        select_clause = SelectClause('select *')
        from_clause = FromClause(f'from {self.update_clause.dataset}')
        where_clause = self.where_clause

        query = Query(select_clause, from_clause, where_clause)

        return query.count()


@dataclass
class DeleteClause:
    leading_word: str

    def __init__(self, *args):
        self.leading_word = 'delete'

    def __str__(self):
        return self.leading_word

# TODO: Find commonality between this and a Query
@dataclass
class DeleteStatement:
    """ docstring tbd """
    sql_str: SqlStr = dataclass_field(repr=False)
    delete_clause: DeleteClause
    from_clause: FromClause
    where_clause: WhereClause

    def __init__(self, *args, **kwargs):
        if len(args) == 1:
            if type(args[0]) == str:
                # TODO: Distinguish between s_str and sql_str everywhere
                #       s_str being a snippet? and sql_str a full query sql
                s_str = args[0]
                delete_clause = DeleteClause(s_str)
                from_clause = FromClause(s_str)
                where_clause = WhereClause(s_str) or None

            elif type(args[0]) == list:
                s_str = ''
                # TODO: Accommodate for missing where_clause
                delete_clause = args[0][0]
                from_clause = args[0][1]
                where_clause = args[0][2]

        elif len(args) == 2:
            s_str = ''
            delete_clause = args[0]
            from_clause = args[1]
            where_clause = None

        elif len(args) == 3:
            s_str = ''
            delete_clause = args[0]
            from_clause = args[1]
            where_clause = args[2]

        else:
            s_str = ''
            delete_clause = kwargs.get('delete_clause')
            from_clause = kwargs.get('from_clause')
            where_clause = kwargs.get('where_clause')

        self.sql_str = SqlStr(s_str)
        self.delete_clause = delete_clause
        self.from_clause = from_clause
        self.where_clause = where_clause

    def __str__(self):
        string = str(self.delete_clause)

        string += f' {self.from_clause}'

        if hasattr(self, 'where_clause'):
            if self.where_clause:
                string += f' {self.where_clause}'

        return string

    def count(self):
        select_clause = SelectClause('select *')
        from_clause = self.from_clause
        where_clause = self.where_clause

        query = Query(select_clause, from_clause, where_clause)

        return query.count()
