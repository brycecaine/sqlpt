import sqlparse
from sqlparse.sql import Statement, Token
from sqlparse import tokens as T


def extract(sql):
    sql_elements = sqlparse.parse(sql)

    start_appending = False
    from_clause_tokens = []

    for sql_token in sql_elements[0].tokens:
        """ token attributes and methods: [
            'flatten', 'has_ancestor', 'is_child_of', 'is_group', 'is_keyword',
            'is_whitespace', 'match', 'normalized', 'parent', 'ttype', 'value',
            'within']
        """

        # Append from-clause tokens
        if not sql_token.is_whitespace:
            if sql_token.value == 'from':
                start_appending = True

            if sql_token.value[:5] == 'where':
                start_appending = False

            if start_appending:
                from_clause_tokens.append(sql_token)

    whitespace = Token(T.Whitespace, ' ')

    from_clause_tokens_with_whitespace = []

    # Insert whitespace
    for i, item in enumerate(from_clause_tokens):
        from_clause_tokens_with_whitespace.append(item)

        if i % 1 == 0:
            from_clause_tokens_with_whitespace.append(whitespace)

    # Construct from-clause string
    from_clause_statement = Statement(from_clause_tokens_with_whitespace)
    from_clause_statement_str = str(from_clause_statement).strip()

    return from_clause_statement_str


def fuse():
    print('fusing')

    return True
