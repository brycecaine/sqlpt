from sqlparse import parse as sql_parse, sql, tokens as T
from email import header
from typing import operator


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
        return self.name

    def __eq__(self, other):
        equal = False

        if isinstance(other, self.__class__):
            equal = self.name == other.name

        return equal


class Join:
    def __init__(self, left_table=None, right_table=None, comparison=None):
        self.left_table = left_table
        self.comparison = comparison
        self.right_table = right_table
    
    def __str__(self):
        right_join_str = f'from {self.left_table}'
        left_join_str = f'join {self.right_table} on {self.comparison}'

        if not self.right_table:
            join_str = right_join_str

        elif not self.left_table:
            join_str = left_join_str

        else:
            join_str = f'{right_join_str} {left_join_str}'

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

            comparison_equal = self.comparison.value == other.comparison.value

            equal = tables_equal and comparison_equal

        return equal


class FromClause:
    def __init__(self, joins):
        self.joins = joins

    def __str__(self):
        from_clause_str = ''

        for join in self.joins:
            from_clause_str += str(join)

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

        for comparison in self.comparisons:
            where_clause_str += str(comparison)

        return where_clause_str


class Query:
    def __init__(self, sql_str):
        self.sql_str = sql_str

    def remove_whitespace(self, token_list):
        tokens = [x for x in token_list if not x.is_whitespace]

        return tokens

    def tokenize(self):
        sql_statement = sql_parse(self.sql_str)
        all_tokens = sql_statement[0].tokens
        tokens = self.remove_whitespace(all_tokens)

        return tokens

    def from_clause(self):
        sql_elements = sql_parse(self.sql_str)

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
                left_table = None

                if i == 0:
                    left_table = first_table

                right_table = Table(str(from_clause_tokens[i+1]))
                comparison = from_clause_tokens[i+3]
                join = Join(left_table, right_table, comparison)

                joins.append(join)

        from_clause = FromClause(joins)

        return from_clause

    def where_clause(self):
        sql_elements = sql_parse(self.sql_str)

        comparisons = []

        for sql_token in sql_elements[0].tokens:
            if type(sql_token) == sql.Where:
                where_tokens = sql_token.tokens

                for where_token in where_tokens:
                    if type(where_token) == sql.Comparison:

                        comparison_tokens = self.remove_whitespace(where_token.tokens)
                        for i, comparison_token in enumerate(comparison_tokens):
                            if type(comparison_token) == sql.Identifier:
                                left_expression = comparison_token.value
                                operator = comparison_tokens[i+1].value
                                right_expression = comparison_tokens[i+2].value

                                comparison = Comparison(
                                    left_expression, operator, right_expression)

                                comparisons.append(comparison)

        where_clause = WhereClause(comparisons)

        return where_clause
