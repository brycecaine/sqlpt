select tbl_stu.id,
       name,
       major
  from tbl_stu_crs
  join tbl_stu
    on tbl_stu.id = tbl_stu_crs.stu_id
 where stu_sem = '2020-Sp'
   and enrl = 1;
