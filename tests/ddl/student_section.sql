create table student_section (
    id int primary key not null,
    student_id int not null,
    term_id int not null,
    section_id int not null
);

insert into student_section values (1, 1, 1, 1);
insert into student_section values (2, 2, 2, 4);
insert into student_section values (3, 3, 1, 2);
insert into student_section values (4, 4, 2, 3);
