from email import header
from typing import operator


class Table:
    def __init__(self, name):
        self.name = name

    def __str__(self):
        return self.name


class Field:
    def __init__(self, name):
        self.name = name

    def __str__(self):
        return self.name


class Join:
    def __init__(self, table, comparison, join_type=None):
        self.table = table
        self.comparison = comparison
        self.join_type = join_type


class FromClause:
    def __init__(self, table, joins):
        self.table = table
        self.joins = joins

    def __str__(self):
        from_clause_str = f'from {self.table}'

        for join in self.joins:
            join_type = f' {join.join_type}'

            if join.join_type == None or join.join_type== 'inner':
                join_type = ''

            from_clause_str += f'{join_type} join {join.table}'

            from_clause_str += f' on {join.comparison}'

        return from_clause_str
