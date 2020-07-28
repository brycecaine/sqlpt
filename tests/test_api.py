from sqlparse.sql import Comparison, Identifier
import unittest
from sqlparse.sql import Statement, Token, TokenList
from sqlparse import tokens as T

from sqlpt import extract, extract, query


class TestApi(unittest.TestCase):
    def test_extract(self):
        sql = ("select id, "
               "       name "
               "  from tbl_stu "
               "  join tbl_stu_crs "
               "    on tbl_stu.id = tbl_stu_crs.stu_id "
               " where stu_sem = '2020-Sp' "
               "   and enrl = 1;")

        whitespace = Token(T.Whitespace, ' ')

        table_1 = query.Table('tbl_stu')
        table_2 = query.Table('tbl_stu_crs')

        token_1 = Token(T.Name, 'tbl_stu.id')
        token_list_1 = TokenList([token_1])
        field_1 = Identifier(token_list_1)

        comparison_token = Token(T.Operator, '=')
        comparison_list = TokenList([comparison_token])
        comparison_1 = Identifier(comparison_list)

        token_2 = Token(T.Name, 'tbl_stu_crs.stu_id')
        token_list_2 = TokenList([token_2])
        field_2 = Identifier(token_list_2)

        join_comparison = Comparison(
            [field_1, whitespace, comparison_1, whitespace, field_2])

        join_1 = query.Join(table_2, join_comparison)

        joins = [join_1]

        from_clause = query.FromClause(table_1, joins)

        from_clause_expected = str(from_clause)

        from_clause_actual = str(extract(sql))

        self.assertEqual(from_clause_actual, from_clause_expected)


if __name__ == '__main__':
    unittest.main()
