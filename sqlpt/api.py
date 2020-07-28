import sqlparse
from sqlparse.sql import Statement, Token
from sqlparse import tokens as T
from sqlpt.query import FromClause, Join, Table


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
    main_table = Table(table_name)

    joins = []

    for i, item in enumerate(from_clause_tokens):
        if str(item) == 'join':
            table = from_clause_tokens[i+1]
            comparison = from_clause_tokens[i+3]
            join = Join(table, comparison)

            joins.append(join)

    from_clause = FromClause(main_table, joins)

    return from_clause


def fuse():
    print('fusing')

    return True
