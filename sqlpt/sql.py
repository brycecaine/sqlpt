""" docstring tbd """

import re
from copy import deepcopy
from dataclasses import dataclass
from dataclasses import field as dataclass_field

import sqlparse
from sqlalchemy import create_engine, exc, inspect
from sqlparse.sql import Comparison as SqlParseComparison
from sqlparse.sql import Identifier, IdentifierList, Parenthesis, Token, Where

from sqlpt.service import (get_join_clause_kind, get_truth_table_result, is_join_clause,
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

        group_by_clause = GroupByClause(field_names=field_names)

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

        query = Query(sql_str=f'select rowid from {self.name}', db_conn_str=self.db_conn_str)
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
                comparison = Comparison(token_list=comparison_token_list)
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

    def __init__(self, s_str=None, leading_word=None, expression=None, token_list=None):
        if s_str:
            token_list = self.parse_expression_clause(s_str)
            leading_word, expression = self.get_expression_clause_parts(token_list)

        elif token_list:
            leading_word, expression = self.get_expression_clause_parts(token_list)

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
    """A sql join clause's on clause"""

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

        token_list = self.parse_on_clause(s_str)

        return token_list


@dataclass
class JoinClause:
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

        join_clause_str = f'{self.simple_kind} {dataset_str} {self.on_clause}'

        return join_clause_str

    @property
    def simple_kind(self):
        """Returns a simplified join kind (inner join to just join, or left/right join)
        
        Returns:
            kind (str): The simplified join kind
        """

        kind = 'join' if self.kind == 'inner' else f'{self.kind} join'

        return kind

    def is_equivalent_to(self, other):
        """Returns equivalence of the join-clause logic; this is different than checking
            for equality (__eq__)

        Args:
            other (JoinClause): Another join clause to compare to
            
        Returns:
            equivalent (bool): Whether the join clauses are logically equivalent
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
    join_clauses: list

    # Can either have s_str and optional db_conn_str, or from_dataset and join clauses
    # TODO: Raise exception if args aren't passed in properly (do this for other methods too)
    def __init__(self, s_str=None, from_dataset=None, join_clauses=None, db_conn_str=None):
        if s_str:
            token_list = self._parse_from_clause_from_str(s_str)

            from_dataset, join_clauses = self._parse_from_clause_from_tokens(
                token_list, db_conn_str)

        self.from_dataset = from_dataset
        self.join_clauses = join_clauses or []

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

            for join_clause in self.join_clauses:
                from_clause_str += f' {join_clause}'

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
        """Parses and returns a from-dataset and join clauses based on the given token_list
        
        Args:
            s_str (str): A short sql string representing a from clause

        Returns:
            token_list (list): A token list
        """

        from_dataset = None
        join_clauses = []

        if token_list:
            # Remove 'from' keyword for now
            token_list.pop(0)

            # Get from_dataset
            token = token_list.pop(0)
            from_dataset = get_dataset(token, db_conn_str)

            # Construct join_clauses
            kind = None
            dataset = None
            on_tokens = []

            for token in token_list:
                # Parse join_clause token
                if is_join_clause(token):
                    # Create join_clause object with previously populated values
                    # if applicable, and clear out values for a next one
                    if kind and dataset and on_tokens:
                        join_clause_kind = deepcopy(str(kind))
                        join_clause_dataset = deepcopy(str(dataset))
                        join_clause_on_clause = OnClause(token_list=on_tokens)

                        join_clause = JoinClause(kind=join_clause_kind,
                                                 dataset=join_clause_dataset,
                                                 on_clause=join_clause_on_clause)

                        join_clauses.append(join_clause)

                        kind = None
                        dataset = None
                        on_tokens = []

                    kind = get_join_clause_kind(token)

                    continue

                # Parse dataset token
                if type(token) in (Identifier, Parenthesis):
                    dataset = get_dataset(token, db_conn_str)

                    continue

                # Parse comparison token
                on_tokens.append(token)

            # Create the last join_clause
            if kind and dataset and on_tokens:
                on_clause = OnClause(token_list=on_tokens)

                join_clause = JoinClause(kind=kind, dataset=dataset, on_clause=on_clause)
                join_clauses.append(join_clause)

        return from_dataset, join_clauses

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
                          (self.from_dataset == other.get_first_join_clause_dataset()
                           and
                           other.from_dataset == self.get_first_join_clause_dataset()))

            # FUTURE: Work this out
            if equivalent:
                for self_join_clause in self.join_clauses:
                    for other_join_clause in other.join_clauses:
                        if not self_join_clause.is_equivalent_to(other_join_clause):
                            equivalent = False
                            break

                for other_join_clause in other.join_clauses:
                    for self_join_clause in self.join_clauses:
                        if not other_join_clause.is_equivalent_to(self_join_clause):
                            equivalent = False
                            break

        return equivalent

    def get_first_join_clause_dataset(self):
        """Returns the first join_clause's dataset for inner joins but None for left/right
            join_clauses

        Returns:
            first_join_clause_dataset (DataSet): The first join's dataset
        """

        first_join_clause = self.join_clauses[0]

        if first_join_clause.kind in ('inner', 'join_clause'):
            first_join_clause_dataset = first_join_clause.dataset
        else:
            first_join_clause_dataset = None

        return first_join_clause_dataset

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

        for i, join_clause in enumerate(self.join_clauses):
            enumerated_comparisons = enumerate(
                join_clause.on_clause.expression.comparisons)

            for j, comparison in enumerated_comparisons:
                if s_str in comparison.left_term:
                    location_tuple = (
                        'from_clause', 'join_clauses', i, 'on_clause', 'expression',
                        'comparisons', j, 'left_term')
                    locations.append(location_tuple)
                elif s_str in comparison.right_term:
                    location_tuple = (
                        'from_clause', 'join_clauses', i, 'on_clause', 'expression',
                        'comparisons', j, 'right_term')
                    locations.append(location_tuple)

        return locations

    def remove_join_clause(self, join_clause):
        """Removes a join_clause from the from clause
        
        Args:
            join_clause (Join): A join
        
        Returns:
            None
        """

        self.join_clauses.remove(join_clause)


@dataclass
class Comparison:
    """ docstring tbd """
    bool_conjunction: str = dataclass_field(repr=False)
    bool_sign: str = dataclass_field(repr=False)
    left_term: str
    operator: str
    right_term: str

    def __init__(self, s_str=None, token_list=None, left_term=None, operator=None, right_term=None):
        if s_str:
            statement = sqlparse.parse(s_str)
            sqlparse_comparison = statement[0].tokens[0]
            comparison_tokens = (
                remove_whitespace(sqlparse_comparison.tokens))

            elements = [comparison_token.value
                        for comparison_token in comparison_tokens]

        # FUTURE: De-support token_list arg?
        elif token_list:
            sqlparse_comparison = token_list[0]

            elements = []

            for sqlparse_comparison in token_list:
                if type(sqlparse_comparison) == SqlParseComparison:
                    comparison_tokens = remove_whitespace(sqlparse_comparison.tokens)

                    els = [comparison_token.value
                           for comparison_token in comparison_tokens]

                    elements.extend(els)

                else:
                    elements.append(sqlparse_comparison.value)

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

        left_term = left_term or elements[0]
        operator = operator or elements[1]
        right_term = right_term or elements[2]

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
        """Returns equivalence of the comparison logic; this is different than checking
            for equality (__eq__)

        Args:
            other (Comparison): Another comparison to compare to
            
        Returns:
            equivalent (bool): Whether the comparisons are logically equivalent
        """

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

    # FUTURE: Test this in test_classes
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

    # FUTURE: Test this in test_classes
    def parse_expression_clause(self, sql_str):
        """Returns a where-clause token list based on the given string
        
        Args:
            s_str (str): A short sql string representing a where clause

        Returns:
            token_list (list): A token list
        """

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

    def __init__(self, s_str=None, field_names=None):
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

        token_list = self.parse_having_clause(s_str)

        return token_list


# TODO: Add suport for insert statements


# TODO: Add suport for OrderByClause
@dataclass
class Query(DataSet):
    """A sql query"""
    sql_str: str = dataclass_field(repr=False)
    select_clause: SelectClause
    from_clause: FromClause
    where_clause: WhereClause
    group_by_clause: GroupByClause
    having_clause: HavingClause

    def __init__(self, sql_str=None, select_clause=None, from_clause=None,
                 where_clause=None, group_by_clause=None, having_clause=None,
                 db_conn_str=None):

        if sql_str:
            # Accommodate subqueries surrounded by parens
            sql_str = sql_str[1:-1] if sql_str[:7] == '(select' else sql_str

            # TODO: Do away with these "or None"s?
            select_clause = SelectClause(sql_str) or None
            from_clause = FromClause(s_str=sql_str, db_conn_str=db_conn_str) or None
            # TODO: Make sure every class is instantiated with keyword arguments (but don't accept a general kwargs in init in order to be explicit)
            where_clause = WhereClause(s_str=sql_str) or None
            group_by_clause = None
            having_clause = None

        self.sql_str = sql_str
        self.select_clause = select_clause
        self.from_clause = from_clause
        self.where_clause = where_clause
        self.group_by_clause = group_by_clause
        self.having_clause = having_clause
        self.db_conn_str = db_conn_str

    def __hash__(self):
        return hash(str(self))

    # FUTURE: Also implement is_equivalent_to
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
        """Returns whether two optional clauses are equal

        Args:
            other (Query): Another query to compare clauses against 
            kind (str): The kind of clause to compare against
        
        Returns:
            clauses_equal (bool): True/False whether the clauses are equal
        """

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


    # TODO: Consider standardizing this to locate_field?
    def locate_column(self, s_str):
        """Returns a columns's "location" in the query
        
        Args:
            s_str (str): A short sql string representing a column to be located
            
        Returns:
            locations (list): The resulting list of column locations
        """

        locations = self.select_clause.locate_field(s_str)

        if self.from_clause:
            locations.extend(self.from_clause.locate_field(s_str))

        if self.where_clause:
            locations.extend(self.where_clause.locate_field(s_str))

        return locations

    def delete_node(self, coordinates):
        """Deletes a node from the sql query
        
        Args:
            coordinates (list): A list of coordinate tuples
        
        Returns:
            self (Query): The resulting query
        """

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
        """Locates and returns coordinates of invalid columns
        
        Returns:
            invalid_column_coordinates (list): A list of invalid coordinate tuples
        """

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
        """Removes a node from the query
        
        Returns:
            cropped_query (Query): The resulting query
        """

        invalid_column_coordinates = self.locate_invalid_columns()
        cropped_query = self.delete_node(invalid_column_coordinates)

        return cropped_query

    def parameterize_node(self, coordinates):
        """Parameterizes a node in the query
        
        Args:
            coordinates (list): A list of coordinate tuples

        Returns:
            self (Query): The resulting query
        """

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
        """Parameterizes a the query
        
        Returns:
            parameterized_query (Query): The resulting query
        """

        # self.where_clause.parameterize(parameter_fields)
        invalid_column_coordinates = self.locate_invalid_columns()
        parameterized_query = self.parameterize_node(
            invalid_column_coordinates)

        return parameterized_query

    def run(self, **kwargs):
        """Runs (executes) the query

        Args:
            kwargs (kwargs): Keyword arguments to pass as parameters when executing

        Returns:
            row_dicts (list): The resulting list of dictionaries from running the query
        """

        rows = []

        with self.db_conn.connect() as db_conn:
            rows = db_conn.execute(str(self), **kwargs)
            row_dicts = QueryResult()

            for row in rows:
                row_dict = dict(row._mapping.items())
                row_dicts.append(row_dict)

        return row_dicts

    def count(self, **kwargs):
        """Counts the rows from running the query

        Args:
            kwargs (kwargs): Keyword arguments to pass as parameters when executing

        Returns:
            ct (int): The count of resulting rows from running the query
        """

        ct = len(self.run(**kwargs))

        return ct

    def counts(self):
        """Counts the rows from running the query and tables within the query

        Returns:
            counts_dict (dict): The count of rows from running the query and its tables
        """

        counts_dict = {}
        query_count = self.count()
        counts_dict['query'] = query_count

        from_dataset = self.from_clause.from_dataset
        counts_dict[from_dataset.name] = from_dataset.count()

        for join_clause in self.from_clause.join_clauses:
            counts_dict[join_clause.dataset.name] = join_clause.dataset.count()

        return counts_dict

    def rows_exist(self, **kwargs):
        """Checks if rows exist in the query
        
        Args:
            kwargs (kwargs): Keyword arguments to pass as parameters when executing

        Returns:
            rows_exist_bool (bool): Whether rows exist in the query results or not
        """

        row_count = self.count(**kwargs)

        rows_exist_bool = True if row_count != 0 else False

        return rows_exist_bool

    def scalarize(self):
        """Converts a query's left-join-statement fields to scalar subqueries
        
        Returns:
            scalarized_query (Query): The resulting query
        """

        join_clauses_to_remove = []

        for join_clause in self.from_clause.join_clauses:
            if join_clause.kind == 'left':
                column_names = join_clause.dataset.get_column_names()

                for field in self.select_clause.fields:
                    if field.expression in column_names:
                        self.select_clause.remove_field(field)

                        subquery_select_clause = SelectClause(fields=[field])
                        subquery_from_clause = FromClause(
                            from_dataset=join_clause.dataset, db_conn_str=join_clause.dataset.db_conn_str)
                        subquery_where_clause = WhereClause(
                            expression=join_clause.on_clause.expression)
                        subquery = Query(
                            select_clause=subquery_select_clause,
                            from_clause=subquery_from_clause,
                            where_clause=subquery_where_clause,
                            db_conn_str=join_clause.dataset.db_conn_str)

                        alias = field.alias or field.expression
                        expression = f'({str(subquery)})'
                        subquery_field = Field(
                            expression=expression, alias=alias, query=subquery, db_conn_str=join_clause.dataset.db_conn_str)
                        self.select_clause.add_field(subquery_field)

                        join_clauses_to_remove.append(join_clause)

        for join_clause_to_remove in join_clauses_to_remove:
            self.from_clause.remove_join_clause(join_clause_to_remove)

        scalarized_query = Query(
            select_clause=self.select_clause,
            from_clause=self.from_clause,
            where_clause=self.where_clause)

        return scalarized_query

    def is_leaf(self):
        """Checks if a query is a leaf node, meaning it doesn't contain any subqueries

        Returns:
            not contains_subqueries (bool): Whether the query doesn't contain subqueries
        """

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
            for join_clause in self.from_clause.join_clauses:
                if type(join_clause.dataset) == Query:
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
        """Formats and returns sql in a human-readable format
        
        Returns:
            formatted_sql (str): The formatted sql
        """

        formatted_sql = sqlparse.format(self.__str__())

        return formatted_sql

    def output_sql_file(self, path):
        """Outputs the query to a sql file
        
        Args:
            path (str): The path of the destination sql file

        Returns:
            None
        """

        with open(path, 'wt') as sql_file:
            sql_file.write(self.format_sql())

    def subquery_str(self):
        """Get the subquery version of the query
        
        Returns:
            string (str): the subquery version of the query (wrapped with parens)
        """

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

    # FUTURE: Make a query.is_equivalent_to instance method


@dataclass
class Field:
    """A field in a query"""
    expression: str
    alias: str
    query: Query = dataclass_field(repr=False)

    def __init__(self, s_str=None, expression=None, alias=None, query=None, db_conn_str=None):
        db_conn_str = None

        if s_str:
            expression, alias, query = parse_field(s_str, 'tuple', db_conn_str)

        else:
            if expression:
                # TODO: Unhardcode this
                if query:
                    if query.from_clause:
                        query.from_clause.from_dataset.db_conn_str = db_conn_str

                else:
                    query = Query(sql_str=expression, db_conn_str=db_conn_str)

            else:
                query = None

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
    """An update clause in an update statement"""
    dataset: DataSet

    def __init__(self, s_str=None):
        dataset = None

        sql_parts = s_str.split()

        if len(sql_parts) == 1:
            dataset = sql_parts[0]

        else:
            dataset = sql_parts[1]

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

        token_list = self.parse_set_clause(s_str)

        return token_list


@dataclass
class UpdateStatement:
    """An update sql statement"""
    sql_str: str = dataclass_field(repr=False)
    update_clause: UpdateClause
    set_clause: SetClause
    where_clause: WhereClause
    db_conn_str: str

    def __init__(self, s_str=None, update_clause=None, set_clause=None, where_clause=None, db_conn_str=None):
        if s_str:
            update_clause = UpdateClause(s_str) or None
            set_clause = SetClause(s_str=s_str) or None
            where_clause = WhereClause(s_str=s_str) or None

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
        """Returns the count related to the update statement
        
        Returns:
            ct (int): The count related to the update statement
        """

        select_clause = SelectClause('select *')
        from_clause = FromClause(f'from {self.update_clause.dataset}')
        where_clause = self.where_clause

        query = Query(
            select_clause=select_clause,
            from_clause=from_clause,
            where_clause=where_clause,
            db_conn_str=self.db_conn_str)

        ct = query.count()

        return ct


@dataclass
class DeleteClause:
    """An update clause in an update statement"""
    def __str__(self):
        return 'delete'


@dataclass
class DeleteStatement:
    """A delete sql statement"""
    sql_str: str = dataclass_field(repr=False)
    delete_clause: DeleteClause
    from_clause: FromClause
    where_clause: WhereClause
    db_conn_str: str

    def __init__(self, s_str=None, delete_clause=None, from_clause=None, where_clause=None, db_conn_str=None):
        if s_str:
            delete_clause = DeleteClause() or None
            from_clause = FromClause(s_str=s_str) or None
            where_clause = WhereClause(s_str=s_str) or None

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

    # TODO: Consider a single count() method for Select, Update, Delete statements
    def count(self):
        """Returns the count related to the delete statement
        
        Returns:
            ct (int): The count related to the delete statement
        """

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
        dataset = Query(sql_str=sql_str, db_conn_str=db_conn_str)

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
    query = Query(sql_str=expression, db_conn_str=db_conn_str)

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
