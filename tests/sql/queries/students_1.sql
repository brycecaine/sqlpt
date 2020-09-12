select student.id,
       major,
       subject,
       course_number,
       section_number
  from student
  join student_section
    on student.id = student_section.student_id
   and student.term_id = student_section.term_id
  join term
    on student.term_id = term.id
  join section
    on student_section.section_id = section.id
   and student_section.term_id = section.term_id
 where term.code = '2020SU'
   and student.enrolled = 1;
