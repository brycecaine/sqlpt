create table student (
    id int primary key not null,
    person_id int not null,
    term_id varchar not null,
    enrolled int not null,
    major varchar
);

insert into student values (1, 1, 1, 1, 'MATH');
insert into student values (2, 1, 2, 1, 'MATH');
insert into student values (3, 2, 1, 1, 'ENGL');
insert into student values (4, 2, 2, 1, 'ENGL');
