select student.id,
       term.code,
       age,
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
  left
  join (select id,
               id_number,
               (date('now') - birth_date) / 365 age
          from person) person_with_age
    on student.person_id = person_with_age.id
 where person_with_age.id_number = '123456'
   and student.enrolled = 1;
