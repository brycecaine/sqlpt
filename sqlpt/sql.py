""" docstring tbd """

import re
from copy import deepcopy
from dataclasses import dataclass
from dataclasses import field as dataclass_field

import sqlparse
from sqlalchemy import create_engine, exc, inspect
from sqlparse.sql import Comparison as SqlParseComparison
from sqlparse.sql import Identifier, IdentifierList, Parenthesis, Token, Where

from sqlpt.service import (
    get_join_kind, get_truth_table_result, is_equivalent, is_join,
    remove_whitespace)


class QueryResult(list):
    """ docstring tbd """
    def count(self):
        return len(self)


@dataclass
class DataSet:
    """An abstract dataset; can be a table or query"""

    @property
    def db_conn(self):
        """Returns the database connection engine based on a connection string

        Returns:
            db_conn (Engine): A sqlalchemy database Engine instance
        """

        db_conn = create_engine(self.db_conn_str) if self.db_conn_str else None

        return db_conn

    def rows_unique(self, field_names):
        """Returns the dataset's row-uniqueness based on field_names

        Args:
            field_names (list): A list of the dataset's field names 
        
        Returns:
            unique (bool): A dataset's row-uniqueness

        Raises:
            Exception: If type is DataSet; an abstract DataSet has no rows
        """

        if type(self) == DataSet:
            raise Exception('Cannot check uniqueness of rows on an abstract DataSet')

        fields = [Field(field_name) for field_name in field_names]
        select_clause = SelectClause(fields=fields)
        select_clause.add_field('count(*)')

        from_clause = FromClause(from_dataset=self)

        group_by_clause = GroupByClause(field_names)

        having_clause = HavingClause(s_str='having count(*) > 1')

        query = Query(select_clause=select_clause,
                      from_clause=from_clause,
                      group_by_clause=group_by_clause,
                      having_clause=having_clause,
                      db_conn_str=self.db_conn_str)

        unique = not query.rows_exist()

        return unique


@dataclass
class Table(DataSet):
    """A database table"""

    name: str
    db_conn_str: str = None

    def __hash__(self):
        return hash(str(self))

    def __str__(self):
        string = self.name if hasattr(self, 'name') else ''

        return string

    def count(self):
        """Returns the row count for the table

        Returns:
            row_count (int): The table's row count
        """

        query = Query(f'select rowid from {self.name}', self.db_conn_str)
        row_count = query.count()

        return row_count

    def get_columns(self):
        """Returns the columns metadata for the table
        
        Returns:
            columns (list): A list of dicts containing the columns metadata
        """

        insp = inspect(self.db_conn)
        columns = insp.get_columns(self.name)

        return columns

    def get_column_names(self):
        """Returns the column names for the table
        
        Returns:
            column_names (list): A list of column names
        """

        columns = self.get_columns()
        column_names = [col_dict['name'] for col_dict in columns]

        return column_names

    def is_equivalent_to(self, other):
        """Returns equivalence of the tables; this is different
            than checking for equality (__eq__)

        Args:
            other (Table): Another table to compare to
            
        Returns:
            equivalent (bool): Whether the table are logically equivalent
        """

        equivalent = False

        if isinstance(other, self.__class__):
            equivalent = self.name == other.name

        return equivalent


@dataclass
class SelectClause:
    """A select clause of a sql query"""
    fields: list

    def __init__(self, s_str=None, fields=None):
        self.fields = parse_select_clause(s_str) if s_str else fields

    def __hash__(self):
        return hash(str(self))

    def __bool__(self):
        if self.fields:
            return True

        return False

    def __str__(self):
        field_names_str = ', '.join(self.field_names)
        select_clause_str = f"select {field_names_str}"

        return select_clause_str

    @property
    def field_names(self):
        """Returns a list of field names from the select clause

        Returns:
            field_names
        """

        field_names = [str(field) for field in self.fields]

        return field_names

    def add_field(self, s_str=None, field=None):
        """Adds a field to the select clause and returns the resulting field list
        
        Args:
            s_str (str): A short sql string representing a field to be added
            field (Field): A field to be added
            
        Returns:
            self.fields (list): The resulting field list after the field has been added
        
        Raises:
            Exception: If neither s_str or field are provided
        """

        if s_str is None and field is None:
            raise Exception('Either s_str or field need to have values')

        field = Field(s_str) if s_str else field

        self.fields.append(field)

        return self.fields

    def remove_field(self, s_str=None, field=None):
        """Removes a field from the select clause and returns the resulting fields list
        
        Args:
            s_str (str): A short sql string representing a field to be removed
            field (Field): A field to be removed
            
        Returns:
            self.fields (list): The resulting field list after the field has been
                removed
        
        Raises:
            Exception: If neither s_str or field are provided
        """

        if s_str is None and field is None:
            raise Exception('Either s_str or field need to have values')

        field = Field(s_str) if s_str else field

        self.fields.remove(field)

        return self.fields

    def locate_field(self, s_str):
        """Returns a field's "location" in the select clause
        
        Args:
            s_str (str): A short sql string representing a field to be located
            
        Returns:
            locations (list): The resulting list of field locations
        """

        locations = []

        for i, field in enumerate(self.fields):
            if s_str in field.expression:
                locations.append(('select_clause', 'fields', i))

            if field.query:
                locations.extend(field.query.locate_column(s_str))

        return locations

    def is_equivalent_to(self, other):
        """Returns equivalence ignoring the sort order of the fields; this is different
            than checking for equality (__eq__), which considers field order
        Args:
            other (SelectClause): Another select clause to compare to
            
        Returns:
            equivalent (bool): Whether the select clauses are logically equivalent
        """

        equivalent = False

        if isinstance(other, self.__class__):
            equivalent = set(self.fields) == set(other.fields)

        return equivalent


@dataclass
class Expression:
    """An expression as a list of a = b comparisons"""
    comparisons: list

    def __init__(self, s_str=None, comparisons=None):
        if s_str:
            comparisons = []
            statement = sqlparse.parse(s_str)

            expression_tokens = (
                remove_whitespace(statement[0].tokens))
            comparison_token_list = []
            comparison_token_lists = []

            for token in expression_tokens:
                if type(token) != SqlParseComparison:
                    comparison_token_list.append(token)

                elif type(token) == SqlParseComparison:
                    comparison_token_list.append(token)
                    comparison_token_lists.append(
                        comparison_token_list)  # deepcopy?
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
    """An abstract expression clause; can be an on, where, having, or set clause"""

    leading_word: str  #  = dataclass_field(repr=False)
    expression: Expression

    # TODO: remove args and kwargs
    def __init__(self, s_str=None, leading_word=None, expression=None, token_list=None):
        if s_str:
            token_list = self.parse_expression_clause(s_str)
            leading_word, expression = self.__class__.get_expression_clause_parts(token_list)

        elif token_list:
            leading_word, expression = self.__class__.get_expression_clause_parts(token_list)

        self.leading_word = leading_word
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
        """Parses and returns a token list of the expression parts of an string"""
        raise NotImplementedError

    def is_equivalent_to(self, other):
        """Returns equivalence of the expression logic; this is different than checking
            for equality (__eq__)
        Args:
            other (ExpressionClause): Another expression clause to compare to
            
        Returns:
            equivalent (bool): Whether the expression clauses are logically equivalent
        """

        equivalent = (get_truth_table_result(str(self.expression)) ==
                      get_truth_table_result(str(other.expression)))

        return equivalent

    @staticmethod
    def get_expression_clause_parts(token_list):
        """Returns an expression based on the given token list
        
        Args:
            token_list (list): A list of sqlparse.sql Token instances

        Returns:
            expression (Expression): An Expression instance based on the token list
        """

        # TODO: Set this to None instead of ''?
        expression = ''

        for sql_token in token_list:
            trimmed_sql_token = ' '.join(str(sql_token).split())
            expression += f'{trimmed_sql_token} '

        expression = expression[:-1]

        full_expression_words = expression.split(' ')

        leading_word = None

        if full_expression_words[0] in ('on', 'where', 'having', 'set'):
            leading_word = full_expression_words.pop(0)

        expression = Expression(s_str=' '.join(full_expression_words))

        return leading_word, expression


class OnClause(ExpressionClause):
    """A sql join's on clause"""

    def __init__(self, s_str=None, expression=None, token_list=None):
        super().__init__(s_str=s_str, leading_word='on', expression=expression, token_list=token_list)

    @staticmethod
    def parse_on_clause(s_str):
        """Parses and returns an on-clause token list based on the given string
        
        Args:
            s_str (str): A short sql string representing an on clause

        Returns:
            token_list (list): A token list
        """

        sql_tokens = (
            remove_whitespace(sqlparse.parse(s_str)[0].tokens))

        token_list = []

        for sql_token in sql_tokens:
            token_list.append(sql_token)

        return token_list

    def parse_expression_clause(self, s_str):
        """Returns an on-clause token list based on the given string
        
        Args:
            s_str (str): A short sql string representing an on clause

        Returns:
            token_list (list): A token list
        """

        token_list = self.__class__.parse_on_clause(s_str)

        return token_list


# TODO: Change to JoinClause for consistency
@dataclass
class Join:
    """ docstring tbd """
    kind: str
    dataset: DataSet
    on_clause: OnClause

    def __init__(self, s_str=None, kind=None, dataset=None, on_clause=None):
        if s_str:
            # FUTURE: Implement this
            pass

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

        join_str = f'{self.simple_kind} {dataset_str} {self.on_clause}'

        return join_str

    @property
    def simple_kind(self):
        """Returns a simplified join kind (inner join to just join, or left/right join)
        
        Returns:
            kind (str): The simplified join kind
        """

        kind = 'join' if self.kind == 'inner' else f'{self.kind} join'

        return kind

    def is_equivalent_to(self, other):
        """Returns equivalence of the join logic; this is different than checking
            for equality (__eq__)

        Args:
            other (Join): Another join to compare to
            
        Returns:
            equivalent (bool): Whether the joins are logically equivalent
        """

        equivalent = (self.kind == other.kind
                      and
                      self.dataset.is_equivalent_to(other.dataset)
                      and
                      self.on_clause.is_equivalent_to(other.on_clause))

        return equivalent


# TODO: Align the dataclass attributes with what's in __init__ in all methods
@dataclass
class FromClause:
    """ docstring tbd """
    from_dataset: DataSet
    joins: list

    # Can either have s_str and optional db_conn_str, or from_dataset and joins
    # TODO: Raise exception if args aren't passed in properly (do this for other methods too)
    def __init__(self, s_str=None, from_dataset=None, joins=None, db_conn_str=None):
        if s_str:
            token_list = self._parse_from_clause_from_str(s_str)

            from_dataset, joins = self._parse_from_clause_from_tokens(
                token_list, db_conn_str)

        self.from_dataset = from_dataset
        self.joins = joins or []

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
                from_clause_str += f' {join}'

        return from_clause_str

    @staticmethod
    def _parse_from_clause_from_str(s_str):
        """Parses and returns a from-clause token list based on the given string
        
        Args:
            s_str (str): A short sql string representing a from clause

        Returns:
            token_list (list): A token list
        """

        sql_tokens = remove_whitespace(sqlparse.parse(s_str)[0].tokens)

        token_list = []

        start_appending = False

        for sql_token in sql_tokens:
            if type(sql_token) == Token:
                if sql_token.value.lower() == 'from':
                    start_appending = True

            elif type(sql_token) == Where:
                break

            if start_appending:
                token_list.append(sql_token)

        return token_list

    @staticmethod
    def _parse_from_clause_from_tokens(token_list, db_conn_str=None):
        """Parses and returns a from-dataset and joins based on the given token_list
        
        Args:
            s_str (str): A short sql string representing a from clause

        Returns:
            token_list (list): A token list
        """

        from_dataset = None
        joins = []

        if token_list:
            # Remove 'from' keyword for now
            token_list.pop(0)

            # Get from_dataset
            token = token_list.pop(0)
            from_dataset = get_dataset(token, db_conn_str)

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
                        join_on_clause = OnClause(token_list=on_tokens)

                        join = Join(kind=join_kind,
                                    dataset=join_dataset,
                                    on_clause=join_on_clause)

                        joins.append(join)

                        kind = None
                        dataset = None
                        on_tokens = []

                    kind = get_join_kind(token)

                    continue

                # Parse dataset token
                if type(token) in (Identifier, Parenthesis):
                    dataset = get_dataset(token, db_conn_str)

                    continue

                # Parse comparison token
                on_tokens.append(token)

            # Create the last join
            if kind and dataset and on_tokens:
                on_clause = OnClause(token_list=on_tokens)

                join = Join(kind=kind, dataset=dataset, on_clause=on_clause)
                joins.append(join)

        return from_dataset, joins

    def is_equivalent_to(self, other):
        """Returns equivalence of the from-clause logic; this is different than checking
            for equality (__eq__)

        Args:
            other (FromClause): Another from clause to compare to
            
        Returns:
            equivalent (bool): Whether the from clauses are logically equivalent
        """

        equivalent = False

        if isinstance(other, self.__class__):
            # FUTURE: Allow for equivalence if tables and comparisons are out
            # of order
            equivalent = (self.from_dataset == other.from_dataset
                          or
                          (self.from_dataset == other.get_first_join_dataset()
                           and
                           other.from_dataset == self.get_first_join_dataset()))

            # FUTURE: Work this out
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

    def get_first_join_dataset(self):
        """Returns the first join's dataset for inner joins but None for left/right
            joins

        Returns:
            first_join_dataset (DataSet): The first join's dataset
        """

        first_join = self.joins[0]

        if first_join.kind in ('inner', 'join'):
            first_join_dataset = first_join.dataset
        else:
            first_join_dataset = None

        return first_join_dataset

    def locate_field(self, s_str):
        """Returns a field's "location" in the where clause
        
        Args:
            s_str (str): A short sql string representing a field to be located
            
        Returns:
            locations (list): The resulting list of field locations
        """

        """Returns a from clause's "location" in the sql query
        
        Args:
            s_str (str): A short sql string representing a from clause to be located
            
        Returns:
            locations (list): The resulting list of field locations
        """

        locations = []

        for i, join in enumerate(self.joins):
            enumerated_comparisons = enumerate(
                join.on_clause.expression.comparisons)

            for j, comparison in enumerated_comparisons:
                if s_str in comparison.left_term:
                    location_tuple = (
                        'from_clause', 'joins', i, 'on_clause', 'expression',
                        'comparisons', j, 'left_term')
                    locations.append(location_tuple)
                elif s_str in comparison.right_term:
                    location_tuple = (
                        'from_clause', 'joins', i, 'on_clause', 'expression',
                        'comparisons', j, 'right_term')
                    locations.append(location_tuple)

        return locations

    def remove_join(self, join):
        """Removes a join from the from clause
        
        Args:
            join (Join): A join
        
        Returns:
            None
        """

        self.joins.remove(join)


@dataclass
class Comparison:
    """ docstring tbd """
    bool_conjunction: str = dataclass_field(repr=False)
    bool_sign: str = dataclass_field(repr=False)
    left_term: str
    operator: str
    right_term: str

    def __init__(self, *args, **kwargs):
        if len(args) == 1:
            if type(args[0]) == str:
                comparison_str = args[0]
                statement = sqlparse.parse(comparison_str)
                sqlparse_comparison = statement[0].tokens[0]
                comparison_tokens = (
                    remove_whitespace(sqlparse_comparison.tokens))

                elements = [comparison_token.value
                            for comparison_token in comparison_tokens]

            # FUTURE: De-support list arg
            elif type(args[0]) == list:
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
                bool_conjunction = elements.pop(0)
            elif elements[0] == 'not':
                bool_sign = elements.pop(0)
            else:
                bool_conjunction = ''

            if elements[0] == 'not':
                bool_sign = elements.pop(0)
            else:
                bool_sign = ''

            left_term = elements[0]
            operator = elements[1]
            right_term = elements[2]

        else:
            bool_conjunction = kwargs.get('bool_conjunction')
            bool_sign = kwargs.get('bool_sign')
            left_term = kwargs.get('left_term')
            operator = kwargs.get('operator')
            right_term = kwargs.get('right_term')

        self.bool_conjunction = bool_conjunction
        self.bool_sign = bool_sign
        self.left_term = left_term
        self.operator = operator
        self.right_term = right_term

    def __hash__(self):
        return hash(str(self))

    def __str__(self):
        string = ''

        if self.bool_conjunction:
            string += f'{self.bool_conjunction} '

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


class WhereClause(ExpressionClause):
    """A where clause of a sql query"""

    def __init__(self, s_str=None, expression=None, token_list=None):
        super().__init__(s_str=s_str, leading_word='where', expression=expression, token_list=token_list)

    # TODO: Test this in test_classes
    @staticmethod
    def _parse_where_clause(s_str):
        """Parses and returns a where-clause token list based on the given string
        
        Args:
            s_str (str): A short sql string representing a where clause

        Returns:
            token_list (list): A token list
        """

        sql_tokens = (
            remove_whitespace(sqlparse.parse(s_str)[0].tokens))

        token_list = []

        start_appending = False

        for sql_token in sql_tokens:
            if type(sql_token) == Token:
                if sql_token.value.lower() == 'from':
                    continue

            elif type(sql_token) == Where:
                start_appending = True

            if start_appending:
                token_list.append(sql_token)

        return token_list

    # TODO: Test this in test_classes
    def parse_expression_clause(self, sql_str):
        """Returns a where-clause token list based on the given string
        
        Args:
            s_str (str): A short sql string representing a where clause

        Returns:
            token_list (list): A token list
        """

        # TODO: Make sure all staticmethods are called as self. rather than self.__class__.
        token_list = self._parse_where_clause(sql_str)

        return token_list

    def locate_field(self, s_str):
        """Returns a field's "location" in the where clause
        
        Args:
            s_str (str): A short sql string representing a field to be located
            
        Returns:
            locations (list): The resulting list of field locations
        """

        locations = []

        for i, comparison in enumerate(self.expression.comparisons):
            if s_str in comparison.left_term:
                location_tuple = ('where_clause', 'expression', 'comparisons',
                                  i, 'left_term')
                locations.append(location_tuple)
            elif s_str in comparison.right_term:
                location_tuple = ('where_clause', 'expression', 'comparisons',
                                  i, 'right_term')
                locations.append(location_tuple)

        return locations


class GroupByClause:
    """ docstring tbd """
    field_names: list

    def __init__(self, *args, **kwargs):
        if len(args) == 1:
            if type(args[0]) == list:
                # FUTURE: Allow list of Field objects
                field_names = args[0]

        else:
            field_names = kwargs.get('field_names')

        self.field_names = field_names

    def __str__(self):
        if self.field_names:
            string = 'group by '

            for field_name in self.field_names:
                string += f'{field_name}, '

            # Remove trailing comma and space
            string = string[:-2]

        return string


class HavingClause(ExpressionClause):
    """A having clause of a sql query"""

    def __init__(self, s_str=None, expression=None, token_list=None):
        super().__init__(s_str=s_str, leading_word='having', expression=expression, token_list=token_list)

    @staticmethod
    def parse_having_clause(s_str):
        """Parses and returns a having-clause token list based on the given string
        
        Args:
            s_str (str): A short sql string representing a having clause

        Returns:
            token_list (list): A token list
        """

        sql_tokens = (
            remove_whitespace(sqlparse.parse(s_str)[0].tokens))

        token_list = []

        for sql_token in sql_tokens:
            token_list.append(sql_token)

        return token_list

    def parse_expression_clause(self, s_str):
        """Returns a having-clause token list based on the given string
        
        Args:
            s_str (str): A short sql string representing a having clause

        Returns:
            token_list (list): A token list
        """

        token_list = self.__class__.parse_having_clause(s_str)

        return token_list


@dataclass
class Query(DataSet):
    """ docstring tbd """
    sql_str: str = dataclass_field(repr=False)
    select_clause: SelectClause
    from_clause: FromClause
    where_clause: WhereClause
    group_by_clause: GroupByClause
    having_clause: HavingClause

    def __init__(self, *args, **kwargs):
        select_clause = None
        from_clause = None
        where_clause = None
        group_by_clause = None
        having_clause = None
        db_conn_str = None

        s_str = ''

        if len(args) == 1:
            if type(args[0]) == str:
                s_str = args[0]

                # Accommodate subqueries surrounded by parens
                s_str = s_str[1:-1] if s_str[:7] == '(select' else s_str

                # TODO: Do away with these "or None"s?
                select_clause = SelectClause(s_str) or None
                from_clause = FromClause(s_str=s_str, db_conn_str=db_conn_str) or None
                # TODO: Make sure every class is instantiated with keyword arguments (but don't accept a general kwargs in init in order to be explicit)
                where_clause = WhereClause(s_str=s_str) or None
                group_by_clause = None
                having_clause = None

        elif len(args) == 2:
            if type(args[0]) == str:
                s_str = args[0]
                db_conn_str = args[1]

                # Accommodate subqueries surrounded by parens
                s_str = s_str[1:-1] if s_str[:7] == '(select' else s_str

                # TODO: Do away with these "or None"s?
                select_clause = SelectClause(s_str) or None
                from_clause = FromClause(s_str=s_str, db_conn_str=db_conn_str) or None
                where_clause = WhereClause(s_str=s_str) or None
                group_by_clause = None
                having_clause = None

        sql_str = kwargs.get('sql_str', s_str)
        select_clause = kwargs.get('select_clause', select_clause)
        from_clause = kwargs.get('from_clause', from_clause)
        where_clause = kwargs.get('where_clause', where_clause)
        group_by_clause = kwargs.get('group_by_clause', group_by_clause)
        having_clause = kwargs.get('having_clause', having_clause)
        db_conn_str = kwargs.get('db_conn_str', db_conn_str)

        self.sql_str = sql_str
        self.select_clause = select_clause
        self.from_clause = from_clause
        self.where_clause = where_clause
        self.group_by_clause = group_by_clause
        self.having_clause = having_clause
        self.db_conn_str = db_conn_str

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

    def __bool__(self):
        if self.select_clause:
            return True

        return False

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


    def locate_column(self, s_str):
        """ docstring tbd """
        locations = (
            self.select_clause.locate_field(s_str) +
            self.from_clause.locate_field(s_str) +
            self.where_clause.locate_field(s_str))

        return locations

    def delete_node(self, coordinates):
        """ docstring tbd """
        node = self

        for coordinate in coordinates:
            for component in coordinate:
                if type(component) == str:
                    node = getattr(node, component)

                else:
                    # Delete the nth node, not just the part of the node, which
                    # would break the query (hence the `break`)
                    node.pop(component)
                    break

        return self

    def locate_invalid_columns(self):
        """ docstring tbd """
        invalid_column_coordinates = []

        try:
            self.run()

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
        node = self
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
        # parameter is the right_term, so if the invalid column is the
        # left_term, swap them first and then give the right_term a standard
        # bind-parameter name of :[left_term] (replacing . with _)
        if leaf_node:
            if component == 'left_term':
                leaf_node.left_term = leaf_node.right_term

            leaf_node.right_term = f":{leaf_node.left_term.replace('.', '_')}"

        return self

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
        query_count = self.count()
        counts_dict['query'] = query_count

        from_dataset = self.from_clause.from_dataset
        counts_dict[from_dataset.name] = from_dataset.count()

        for join in self.from_clause.joins:
            counts_dict[join.dataset.name] = join.dataset.count()

        return counts_dict

    def rows_exist(self, **kwargs):
        """ docstring tbd """
        row_count = self.count(**kwargs)

        rows_exist_bool = True if row_count != 0 else False

        return rows_exist_bool

    def scalarize(self):
        """ docstring tbd """
        joins_to_remove = []

        for join in self.from_clause.joins:
            if join.kind == 'left':
                column_names = join.dataset.get_column_names()

                for field in self.select_clause.fields:
                    if field.expression in column_names:
                        self.select_clause.remove_field(field)

                        subquery_select_clause = SelectClause(fields=[field])
                        subquery_from_clause = FromClause(
                            from_dataset=join.dataset, db_conn_str=join.dataset.db_conn_str)
                        subquery_where_clause = WhereClause(
                            expression=join.on_clause.expression)
                        subquery = Query(
                            select_clause=subquery_select_clause,
                            from_clause=subquery_from_clause,
                            where_clause=subquery_where_clause,
                            db_conn_str=join.dataset.db_conn_str)

                        alias = field.alias or field.expression
                        expression = f'({str(subquery)})'
                        subquery_field = Field(
                            expression=expression, alias=alias, query=subquery, db_conn_str=join.dataset.db_conn_str)
                        self.select_clause.add_field(subquery_field)

                        joins_to_remove.append(join)

        for join_to_remove in joins_to_remove:
            self.from_clause.remove_join(join_to_remove)

        scalarized_query = Query(
            select_clause=self.select_clause,
            from_clause=self.from_clause,
            where_clause=self.where_clause)

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
            # FUTURE: Check if a subquery lives in the join's on_clause
            for join in self.from_clause.joins:
                if type(join.dataset) == Query:
                    contains_subqueries = True
                    break

        # FUTURE: Check if a subquery lives in the where_clause

        return not contains_subqueries

    def fuse(self, query):
        """ docstring tbd """
        # FUTURE: Figure out how to fuse from clauses, meaning to merge them, keeping
        #     tables from both and preserving logic as much as possible
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

    def output_sql_file(self, path):
        """ docstring tbd """
        with open(path, 'wt') as sql_file:
            sql_file.write(self.format_sql())

    def subquery_str(self):
        """ docstring tbd """
        string = f'({self.__str__()})'

        return string

    def filter_by_subquery(self, subquery_str, operator, value):
        """ docstring tbd """
        if type(value) == list:
            if operator == '=':
                operator = 'in'

            value = ','.join(f"{item}" for item in value if item)
            value = f'({value})'

        comparison = Comparison(
            left_term=subquery_str, operator=operator, right_term=value)

        self.where_clause.add_comparison(comparison)

    # TODO: Make a query.is_equivalent_to instance method


@dataclass
class Field:
    """ docstring tbd """
    expression: str
    alias: str
    query: Query = dataclass_field(repr=False)

    def __init__(self, *args, **kwargs):
        db_conn_str = None

        if len(args) == 1:
            if type(args[0]) == str:
                field_str = args[0]
                expression, alias, query = parse_field(field_str, 'tuple', db_conn_str)

        else:
            expression = kwargs.get('expression')
            alias = kwargs.get('alias')

            if expression:
                db_conn_str = kwargs.get('db_conn_str')

                # TODO: Unhardcode this
                if 'query' in kwargs:
                    if kwargs['query'].from_clause:
                        kwargs['query'].from_clause.from_dataset.db_conn_str = db_conn_str

                query = kwargs.get('query', Query(expression, db_conn_str))
            else:
                query = None
            db_conn_str = kwargs.get('db_conn_str')

        self.expression = expression
        self.alias = alias
        self.query = query
        self.db_conn_str = db_conn_str

    def __hash__(self):
        return hash(str(self))

    def __str__(self):
        alias = f' {self.alias}' if self.alias else ''
        description = f'{self.expression}{alias}'

        return description


@dataclass
class UpdateClause:
    """ docstring tbd """
    dataset: DataSet

    def __init__(self, *args, **kwargs):
        dataset = None

        if len(args) == 1:
            sql_str = args[0]

            if type(args[0]) == str:
                sql_parts = sql_str.split()

                leading_word = 'update'

                if len(sql_parts) == 1:
                    dataset = sql_parts[0]

                else:
                    dataset = sql_parts[1]

        else:
            leading_word = kwargs.get('leading_word')

        self.leading_word = leading_word

        self.dataset = dataset

    def __str__(self):
        update_clause_str = f'update {self.dataset}'

        return update_clause_str


@dataclass
class SetClause(ExpressionClause):
    """A set clause of an update statement"""

    def __init__(self, s_str=None, expression=None, token_list=None):
        super().__init__(s_str=s_str, leading_word='set', expression=expression, token_list=token_list)

    @staticmethod
    def parse_set_clause(s_str):
        """Parses and returns a set-clause token list based on the given string
        
        Args:
            s_str (str): A short sql string representing a set clause

        Returns:
            token_list (list): A token list
        """

        sql_tokens = (
            remove_whitespace(sqlparse.parse(s_str)[0].tokens))

        token_list = []

        for sql_token in sql_tokens:
            token_list.append(sql_token)

        return token_list

    def parse_expression_clause(self, s_str):
        """Returns a set-clause token list based on the given string
        
        Args:
            s_str (str): A short sql string representing a set clause

        Returns:
            token_list (list): A token list
        """

        token_list = self.__class__.parse_set_clause(s_str)

        return token_list


# TODO: 2022-09-03 Left off here...
@dataclass
class UpdateStatement:
    """ docstring tbd """
    sql_str: str = dataclass_field(repr=False)
    update_clause: UpdateClause
    set_clause: SetClause
    where_clause: WhereClause
    db_conn_str: str

    def __init__(self, *args, **kwargs):
        if len(args) == 1:
            if type(args[0]) == str:
                s_str = args[0]
                update_clause = UpdateClause(s_str) or None
                set_clause = SetClause(s_str=s_str) or None
                where_clause = WhereClause(s_str=s_str) or None

        elif len(args) == 2:
            if type(args[0]) == str:
                s_str = args[0]
                update_clause = UpdateClause(s_str) or None
                set_clause = SetClause(s_str=s_str) or None
                where_clause = WhereClause(s_str=s_str) or None
                db_conn_str = args[1]

        else:
            s_str = ''
            update_clause = kwargs.get('update_clause')
            set_clause = kwargs.get('set_clause')
            where_clause = kwargs.get('where_clause')
            db_conn_str = kwargs.get('db_conn_str')

        self.sql_str = s_str
        self.update_clause = update_clause
        self.set_clause = set_clause
        self.where_clause = where_clause
        self.db_conn_str = db_conn_str

    def __str__(self):
        string = str(self.update_clause)

        string += f' {self.set_clause}'

        if hasattr(self, 'where_clause'):
            if self.where_clause:
                string += f' {self.where_clause}'

        return string

    def count(self):
        """ docstring tbd """
        select_clause = SelectClause('select *')
        from_clause = FromClause(f'from {self.update_clause.dataset}')
        where_clause = self.where_clause

        query = Query(
            select_clause=select_clause,
            from_clause=from_clause,
            where_clause=where_clause,
            db_conn_str=self.db_conn_str)

        return query.count()


@dataclass
class DeleteClause:
    """ docstring tbd """
    leading_word: str

    def __init__(self):
        self.leading_word = 'delete'

    def __str__(self):
        return self.leading_word


@dataclass
class DeleteStatement:
    """ docstring tbd """
    sql_str: str = dataclass_field(repr=False)
    delete_clause: DeleteClause
    from_clause: FromClause
    where_clause: WhereClause
    db_conn_str: str

    def __init__(self, *args, **kwargs):
        if len(args) == 1:
            if type(args[0]) == str:
                s_str = args[0]
                delete_clause = DeleteClause() or None
                from_clause = FromClause(s_str) or None
                where_clause = WhereClause(s_str=s_str) or None

        elif len(args) == 2:
            if type(args[0]) == str:
                s_str = args[0]
                delete_clause = DeleteClause() or None
                from_clause = FromClause(s_str) or None
                where_clause = WhereClause(s_str=s_str) or None
                db_conn_str = args[1]

        else:
            s_str = ''
            delete_clause = kwargs.get('delete_clause')
            from_clause = kwargs.get('from_clause')
            where_clause = kwargs.get('where_clause')
            db_conn_str = kwargs.get('db_conn_str')

        self.sql_str = s_str
        self.delete_clause = delete_clause
        self.from_clause = from_clause
        self.where_clause = where_clause
        self.db_conn_str = db_conn_str

    def __str__(self):
        string = str(self.delete_clause)

        string += f' {self.from_clause}'

        if hasattr(self, 'where_clause'):
            if self.where_clause:
                string += f' {self.where_clause}'

        return string

    def count(self):
        """ docstring tbd """
        select_clause = SelectClause('select *')
        from_clause = self.from_clause
        where_clause = self.where_clause

        query = Query(
            select_clause=select_clause,
            from_clause=from_clause,
            where_clause=where_clause,
            db_conn_str=self.db_conn_str)

        return query.count()


def get_dataset(token, db_conn_str=None):
    """ docstring tbd """
    dataset = None

    if type(token) == Parenthesis:
        sql_str = str(token)[1:-1]
        dataset = Query(sql_str, db_conn_str)

    else:
        dataset = Table(name=str(token), db_conn_str=db_conn_str)

    return dataset


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

    fields = parse_fields_from_token_list(token_list)

    return fields


def parse_field(s_str, return_type='dict', db_conn_str=None):
    """ docstring tbd """
    regex = (
        r'(?P<expression>\'?[\w\*]+\'?(?:\([^\)]*\))?|\([^\)]*\))[ ]?(?P<alias>\w*)')  # noqa
    pattern = re.compile(regex)
    match_obj = re.match(pattern, s_str)

    expression = match_obj.group('expression')
    alias = match_obj.group('alias')
    query = Query(expression, db_conn_str)

    if return_type == 'dict':
        return_val = {'expression': expression, 'alias': alias, 'query': query}

    elif return_type == 'tuple':
        return_val = (expression, alias, query)

    return return_val


def parse_fields(s_str):
    """ docstring tbd """
    sql_str = f'select {s_str}' if s_str[:6] != 'select' else f'{s_str}'

    fields = parse_select_clause(sql_str)

    return fields


def parse_fields_from_token_list(field_token_list):
    """ docstring tbd """
    fields = []

    # FUTURE: Chain the "remove" functionality
    for identifier in remove_whitespace(field_token_list, (';', ',')):

        if str(identifier).lower() != 'select':
            if type(identifier) == IdentifierList:
                for inner_identifier in remove_whitespace(identifier, (',')):
                    field_dict = parse_field(str(inner_identifier))
                    fields.append(Field(**field_dict))
            else:
                field_dict = parse_field(str(identifier))
                fields.append(Field(**field_dict))

    return fields
