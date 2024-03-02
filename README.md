# sqlpt - SQL Probing Tool

**`sqlpt`** is a probing tool for Python that helps to inspect and modify sql statements. Its goal is to allow sql statements to be more visible in the application development process and to allow capturing, reusing, and modifying sql-statement components.


# Example

```sql
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
```

```python
>>> from sqlpt.sql import Query
>>> sql_str = '''
        select name,
               favorite_food
          from person
         where shoe_size = 9;
    '''

>>> query = Query(sql_str)
>>> query
Query(select_clause=SelectClause(fields=[Field(expression='name', alias=''), Field(expression='favorite_food', alias='')]), from_clause=FromClause(from_dataset=Table(name='person'), joins=[]), where_clause=WhereClause(expression=Expression(comparisons=[Comparison(left_term='shoe_size', operator='=', right_term='9')])))

>>> query.count()
1
```

Nothing fancy there, but now let's inspect the from clause for further insight:

```python
>>> query.from_clause
FromClause(from_dataset=Table(name='person'), joins=[])

>>> query.from_clause.from_dataset.count()
2
```

Another quick example before a more comprehensive description--let's probe a scalar subquery in the select clause:

```python
>>> sql_str = '''
        select name,
               favorite_food,
               (select name from country where person.birth_country_id = country.id) country_name
          from person
'''

>>> query = Query(sql_str)

>>> query.select_clause.fields[2].query.crop().count()
195  # ~Number of countries in the world
```

Several more features exist for inspecting and modifying sql queries. Enjoy probing!


# Reasoning

Accurate and well-performing sql queries take careful construction. Having a good understanding of the tables, joins, and filters is essential to forming such queries. `sqlpt` provides tools to inspect areas of sql queries to make more informed design decisions.

`sqlpt` utilizes the very useful Python package [`python-sqlparse`](https://pypi.org/project/sqlparse) and builds upon the idea of parsing sql by converting sql queries and their components (i.e., select, from, where clauses) into objects to help manage, modify, and probe sql queries themselves.

The goal of `sqlpt` is not to be another ORM. Several Python ORMs and other sql-related packages (i.e., [SQLAlchemy](https://pypi.org/project/SQLAlchemy)) already do a masterful job of interfacing with databases and representing database objects as Python objects. Complementarily, `sqlpt` places more of the focus on the sql itself, making it a first-class citizen, in an effort to make it more transparent to the developer. It gives tools to both interact with the actual sql as well as run the sql against a database. The goal is to help developers not by ambiguating sql but by bringing sql to the forefront in the development process and simplifying using it in applications. The hope is that there are no surprises or black boxes when using sql.


# Installation

```bash
pip install sqlpt
```


# Documentation

https://sqlpt.readthedocs.io


# Features

## Probing

- [x] Count rows in a query
- [x] Count rows in underlying datasets
- [x] Count expected rows in an update statement
- [x] Count expected rows in a delete statement
- [x] Identify filters in join and where clauses
- [x] Check table granularity
- [x] Check if query is leaf query
- [x] Ignore dangling parameters
- [x] Locate columns in expressions
- [ ] Generate a diff between sql queries

## Modifying

- [ ] Add select-clause field
- [ ] Remove select-clause field
- [ ] Add from-clause join
- [x] Remove from-clause join
- [ ] Add where-clause filter
- [ ] Remove where-clause filter
- [x] Crop where-clause filter
- [x] Convert left join without where-clause filter to scalar subquery in select clause
- [x] Parameterize query with dangling comparison terms
- [ ] Convert select statement to insert statement
- [ ] Convert select statement to update statement


# Future areas of improvement

## Refactoring
- [ ] Underscore methods and where to locate methods (move some to service)
- [ ] Distinguish between s_str and sql_str everywhere (s_str being a snippet and sql_str a full query sql)
- [ ] Address FUTURE notes in code

## Documentation
- [ ] Document all different ways to construct each clause

## Functionality
- [ ] Support insert statements
- [ ] Support order-by clauses
- [ ] Allow for from-clause equivalence if tables and comparisons are out of order
- [ ] Implement query equivalence
- [ ] Create sql functions from subqueries in select clauses

## Code Quality
- [ ] Allow all classes to accept a single s_str argument or keyword args
- [ ] Raise exceptions if required kwargs aren't passed into __init__ methods
