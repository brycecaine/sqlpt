# query.py
class Table():
    name

class Field():
    name
    table

    can_be_functionalized(self, select_clause)
    functionalize()

class Comparison():
    left_expression
    operator
    right_expression

class Join():
    left_table
    comparison
    right_table

    drives_population()

class SelectClause():
    fields

    fuse()

class FromClause():
    joins

    fuse()

class WhereClause():
    comparisons

    parameterize()
    fuse()

class Query():
    sql_str
    select_clause
    from_clause
    where_clause

    fuse()

