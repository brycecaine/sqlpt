create table section (
    id int primary key not null,
    term_id int not null,
    subject varchar not null,
    course_number varchar not null,
    section_number varchar not null
);

insert into section values (1, 1, 'LOGC', '101', '1');
insert into section values (2, 1, 'LING', '101', '1');
insert into section values (3, 2, 'COMP', '101', '1');
insert into section values (4, 2, 'LITR', '101', '1');
