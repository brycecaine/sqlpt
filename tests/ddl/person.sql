create table person (
    id int primary key not null,
    id_number varchar not null,
    name varchar not null,
    birth_date date not null,
    favorite_food varchar,
    shoe_size number
);

insert into person values (1, '123456', 'Bob Bobson', '2001-01-01', 'lasagna', '11');
insert into person values (2, '123457', 'Jane Janeson', '2002-02-02', 'pad thai', '9');
