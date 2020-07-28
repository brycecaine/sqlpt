import sqlparse
from sqlparse.sql import Identifier, Statement, Token
from sqlparse import tokens as T
from sqlpt.query import FromClause, Join, Table


def tokenize(sql):
    sql_statement = sqlparse.parse(sql)
    all_tokens = sql_statement[0].tokens
    whitespace = Token(T.Whitespace, ' ')
    tokens = [x for x in all_tokens if not x.is_whitespace]

    return tokens


def extract(sql):
    sql_elements = sqlparse.parse(sql)

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


def fuse():
    print('fusing')

    return True
