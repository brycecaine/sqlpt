from typing import operator

from sqlparse import parse as sql_parse, sql, tokens as T
from sqlpt import query


def remove_whitespace(token_list):
    tokens = [x for x in token_list if not x.is_whitespace]

    return tokens


def tokenize(sql_str):
    sql_statement = sql_parse(sql_str)
    all_tokens = sql_statement[0].tokens
    tokens = remove_whitespace(all_tokens)

    return tokens


def extract_from_clause(sql_str):
    sql_elements = sql_parse(sql_str)

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
    first_table = query.Table(table_name)

    joins = []

    for i, item in enumerate(from_clause_tokens):
        if str(item) == 'join':
            left_table = None

            if i == 0:
                left_table = first_table

            right_table = query.Table(str(from_clause_tokens[i+1]))
            comparison = from_clause_tokens[i+3]
            join = query.Join(left_table, right_table, comparison)

            joins.append(join)

    from_clause = query.FromClause(joins)

    return from_clause


def extract_where_clause(sql_str):
    sql_elements = sql_parse(sql_str)

    comparisons = []

    for sql_token in sql_elements[0].tokens:
        if type(sql_token) == sql.Where:
            where_tokens = sql_token.tokens

            for where_token in where_tokens:
                if type(where_token) == sql.Comparison:

                    comparison_tokens = remove_whitespace(where_token.tokens)
                    for i, comparison_token in enumerate(comparison_tokens):
                        if type(comparison_token) == sql.Identifier:
                            left_expression = comparison_token.value
                            operator = comparison_tokens[i+1].value
                            right_expression = comparison_tokens[i+2].value

                            comparison = query.Comparison(
                                left_expression, operator, right_expression)

                            comparisons.append(comparison)

    where_clause = query.WhereClause(comparisons)

    return where_clause
