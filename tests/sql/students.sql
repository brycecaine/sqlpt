select tbl_stu.id,
       name
  from tbl_stu
  join tbl_stu_crs
    on tbl_stu.id = tbl_stu_crs.stu_id
 where stu_sem = '2020-Sp'
   and enrl = 1;
