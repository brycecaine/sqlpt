""" docstring tbd """

import re

import sqlparse
import ttg
from sqlparse.sql import Comparison as SQLParseComparison
from sqlparse.sql import Function, Identifier, IdentifierList, Token


# TODO: Accommodate list of strings (in addition to token_list) everywhere
def remove_whitespace(item_list, addl_chars=()):
    """ docstring tbd """
    tokens = []
    chars_to_exclude = [' ', '\n']
    chars_to_exclude.extend(addl_chars)

    for item in item_list:
        value = item.value if type(item) == Token else item

        if value not in chars_to_exclude:
            tokens.append(item)

    return tokens


def tokenize(sql_str):
    """ docstring tbd """
    sql = sqlparse.parse(sql_str)
    all_tokens = sql[0].tokens
    tokens = remove_whitespace(all_tokens)

    return tokens


def remove_whitespace_from_str(string):
    """ docstring tbd """
    string = ' '.join(string.split())
    string = ' '.join(string.split('\n'))

    return string


def get_function_from_statement(statement):
    """ docstring tbd """
    match = re.match(r'.+\(.*\)', statement)

    function_str = match.group() if match else ''

    return function_str


def get_field_expression(field_str):
    """ docstring tbd """
    field_expression = ''

    if field_str not in (',', ' ', '\n'):
        function_str = get_function_from_statement(field_str)

        if function_str:
            field_expression = function_str

        else:
            field_statement_elements = field_str.rsplit(' ', 1)
            field_expression = field_statement_elements[0]

    return field_expression


def get_field_alias(field_str):
    """ docstring tbd """
    field_alias = ''

    if field_str not in (',', ' ', '\n'):
        function_str = get_function_from_statement(field_str)

        if function_str:
            field_alias = (field_str.replace(function_str, '')
                                    .replace(' as ', '')
                                    .replace(' AS ', '')
                                    .replace(' ', ''))

        else:
            field_statement_elements = field_str.rsplit(' ', 1)

            if len(field_statement_elements) > 1:
                field_alias = field_statement_elements[1]

            else:
                field_alias = ''

    return field_alias


def is_select(item):
    """ docstring tbd """
    item_is_select = False

    if type(item) == Token and 'select' in str(item).lower():
        item_is_select = True

    return item_is_select


def get_field_strs(select_clause_str):
    """ docstring tbd """
    field_strs = []

    sql_elements = sqlparse.parse(select_clause_str)

    sql_tokens = remove_whitespace(sql_elements[0].tokens)

    select_fields = None

    for i, item in enumerate(sql_tokens):
        if is_select(item):
            select_fields = sql_tokens[i+1]

    if type(select_fields) == IdentifierList:
        for identifier in select_fields:
            field_str = str(identifier)

            if field_str and field_str not in (' ', ','):
                field_strs.append(field_str)

    elif type(select_fields) in (Identifier, Function, Token):
        identifier = select_fields
        field_str = str(identifier)

        if field_str:
            field_strs.append(field_str)

    return field_strs


def is_equivalent(object_list_1, object_list_2):
    """ docstring tbd """
    # Check if all of list 1 is equivalent with members of list 2
    list_1_equivalence = {}

    for list_1_object in object_list_1:
        list_1_equivalence[list_1_object] = False

        for list_2_object in object_list_2:
            if list_1_object.is_equivalent_to(list_2_object):
                list_1_equivalence[list_1_object] = True
                break

    list_1_equivalent = all(list_1_equivalence.values())

    # Check if all of list 2 is equivalent with members of list 1
    list_2_equivalence = {}

    for list_2_object in object_list_2:
        list_2_equivalence[list_2_object] = False

        for list_1_object in object_list_1:
            if list_2_object.is_equivalent_to(list_1_object):
                list_2_equivalence[list_2_object] = True
                break

    list_2_equivalent = all(list_2_equivalence.values())

    equivalent = list_1_equivalent and list_2_equivalent

    return equivalent


def is_join(item):
    """ docstring tbd """
    item_is_join = False

    if type(item) == Token and 'join' in str(item).lower():
        item_is_join = True

    return item_is_join


def is_conjunction(item):
    """ docstring tbd """
    item_is_conjunction = False

    item_str = str(item).lower()

    if type(item) == Token and ('on' in item_str or 'and' in item_str):
        item_is_conjunction = True

    return item_is_conjunction


def is_sqlparse_comparison(item):
    """ docstring tbd """
    item_is_comparison = False

    if type(item) == SQLParseComparison:
        item_is_comparison = True

    return item_is_comparison


def get_join_kind(item):
    """ docstring tbd """
    join_kind = 'inner'

    if type(item) == Token and 'left' in str(item).lower():
        join_kind = 'left'

    return join_kind


def get_truth_table_result(expr):
    """ docstring tbd """
    expr_w_parens = re.sub(r'(\w+\s*=\s*\w+)', r'(\1)', expr)
    inputs = [i.replace(' ', '') for i in re.split(r'=|and|or|not', expr)]
    truth_table = ttg.Truths(inputs, [expr_w_parens])

    truth_table_result = []

    for conditions_set in truth_table.base_conditions:
        condition_result = truth_table.calculate(*conditions_set)
        truth_table_result.append(condition_result[-1])

    return truth_table_result
