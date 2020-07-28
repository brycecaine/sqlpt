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
