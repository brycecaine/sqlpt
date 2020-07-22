import unittest

from sqlpt import extract


class TestSum(unittest.TestCase):
    def test_extract(self):
        sql = ("select id, "
               "       name "
               "  from tbl_stu "
               "  join tbl_stu_crs "
               "    on tbl_stu.id = tbl_stu_crs.stu_id "
               " where stu_sem = '2020-Sp' "
               "   and enrl = 1;") 

        reduced_sql_expected = ("from tbl_stu "
                                "join tbl_stu_crs "
                                "on tbl_stu.id = tbl_stu_crs.stu_id")

        reduced_sql_actual = extract(sql)

        self.assertEqual(reduced_sql_actual, reduced_sql_expected)

if __name__ == '__main__':
    unittest.main()