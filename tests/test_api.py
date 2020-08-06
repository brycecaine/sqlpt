import unittest

import sqlparse
from sqlparse import tokens as T
from sqlparse.sql import (Identifier,
                          Statement, Token,
                          TokenList)

from sqlpt.api import extract_from_clause, extract_where_clause, tokenize
from sqlpt.query import Query, Join, Table, FromClause, WhereClause, Field, Comparison


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

        table_1 = Table('tbl_stu')
        table_2 = Table('tbl_stu_crs')

        token_1 = Token(T.Name, 'tbl_stu.id')
        token_list_1 = TokenList([token_1])
        field_1 = Identifier(token_list_1)

        comparison_token = Token(T.Operator, '=')
        comparison_list = TokenList([comparison_token])
        comparison_1 = Identifier(comparison_list)

        token_2 = Token(T.Name, 'tbl_stu_crs.stu_id')
        token_list_2 = TokenList([token_2])
        field_2 = Identifier(token_list_2)

        join_comparison = Comparison(field_1, '=', field_2)

        join_1 = Join(table_1, table_2, join_comparison)

        joins = [join_1]

        from_clause = FromClause(joins)

        from_clause_expected = str(from_clause)
        from_clause_actual = str(extract_from_clause(sql))

        self.assertEqual(from_clause_actual, from_clause_expected)

    def test_compare_sql(self):
        sql_1 = ("select id, "
                 "       name "
                 "  from tbl_stu "
                 "  join tbl_stu_crs "
                 "    on tbl_stu.id = tbl_stu_crs.stu_id "
                 " where stu_sem = '2020-Sp' "
                 "   and enrl = 1;")

        sql_2 = ("select id, "
                 "       name, "
                 "       major "
                 "  from tbl_stu_crs "
                 "  join tbl_stu "
                 "    on tbl_stu.id = tbl_stu_crs.stu_id "
                 " where stu_sem = '2020-Sp' "
                 "   and enrl = 1;")

        from_clause_1 = extract_from_clause(sql_1)
        from_clause_2 = extract_from_clause(sql_2)

        where_clause_1 = extract_where_clause(sql_1)
        where_clause_2 = extract_where_clause(sql_2)

        self.assertEqual(from_clause_1, from_clause_2)
        self.assertEqual(where_clause_1, where_clause_2)

    def test_parse(self):
        sql = ("select id, "
               "       name "
               "  from tbl_stu "
               "  join tbl_stu_crs "
               "    on tbl_stu.id = tbl_stu_crs.stu_id "
               "  join tbl_stu_crs_grd "
               "    on a = b "
               "   and c = d "
               " where stu_sem = '2020-Sp' "
               "   and enrl = 1;")

        sql_tokens = tokenize(sql)

        # print(dir(sql_tokens[-1]))
        # print(sql_tokens[-1].tokens)


class TestQuery(unittest.TestCase):
    def test_extract(self):
        sql = ("select id, "
               "       name "
               "  from tbl_stu "
               "  join tbl_stu_crs "
               "    on tbl_stu.id = tbl_stu_crs.stu_id "
               " where stu_sem = '2020-Sp' "
               "   and enrl = 1;")

        whitespace = Token(T.Whitespace, ' ')

        table_1 = Table('tbl_stu')
        table_2 = Table('tbl_stu_crs')

        token_1 = Token(T.Name, 'tbl_stu.id')
        token_list_1 = TokenList([token_1])
        field_1 = Identifier(token_list_1)

        comparison_token = Token(T.Operator, '=')
        comparison_list = TokenList([comparison_token])
        comparison_1 = Identifier(comparison_list)

        token_2 = Token(T.Name, 'tbl_stu_crs.stu_id')
        token_list_2 = TokenList([token_2])
        field_2 = Identifier(token_list_2)

        join_comparison = Comparison(field_1, '=', field_2)

        join_1 = Join(table_1, table_2, join_comparison)

        joins = [join_1]

        from_clause = FromClause(joins)

        from_clause_expected = str(from_clause)
        query = Query(sql)
        from_clause_actual = str(query.from_clause())

        self.assertEqual(from_clause_actual, from_clause_expected)

    def test_compare_sql(self):
        sql_1 = ("select id, "
                 "       name "
                 "  from tbl_stu "
                 "  join tbl_stu_crs "
                 "    on tbl_stu.id = tbl_stu_crs.stu_id "
                 " where stu_sem = '2020-Sp' "
                 "   and enrl = 1;")

        sql_2 = ("select id, "
                 "       name, "
                 "       major "
                 "  from tbl_stu_crs "
                 "  join tbl_stu "
                 "    on tbl_stu.id = tbl_stu_crs.stu_id "
                 " where stu_sem = '2020-Sp' "
                 "   and enrl = 1;")

        query_1 = Query(sql_1)
        query_2 = Query(sql_2)

        from_clause_1 = query_1.from_clause()
        from_clause_2 = query_2.from_clause()

        where_clause_1 = query_1.where_clause()
        where_clause_2 = query_2.where_clause()

        self.assertEqual(from_clause_1, from_clause_2)
        self.assertEqual(where_clause_1, where_clause_2)

    def test_parse(self):
        sql = ("select id, "
               "       name "
               "  from tbl_stu "
               "  join tbl_stu_crs "
               "    on tbl_stu.id = tbl_stu_crs.stu_id "
               "  join tbl_stu_crs_grd "
               "    on a = b "
               "   and c = d "
               " where stu_sem = '2020-Sp' "
               "   and enrl = 1;")

        query = Query(sql)

        # print(dir(sql_tokens[-1]))
        # print(sql_tokens[-1].tokens)


if __name__ == '__main__':
    unittest.main()
