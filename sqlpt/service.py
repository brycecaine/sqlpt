import re
import sqlparse


def remove_whitespace(token_list):
    tokens = [x for x in token_list if not x.is_whitespace]

    return tokens


# TODO: Choose tokenize version (local or imported)
def tokenize(sql_str):
    sql = sqlparse.parse(sql_str)
    all_tokens = sql[0].tokens
    tokens = remove_whitespace(all_tokens)

    return tokens


def remove_whitespace_from_str(string):
    string = ' '.join(string.split())
    string = ' '.join(string.split('\n'))

    return string


def get_function_from_statement(statement):
    match = re.match(r'.+\(.*\)', statement)

    function_str = match.group() if match else ''

    return function_str
