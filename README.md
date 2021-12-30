# sqlpt - SQL Probing Tool

**`sqlpt`** is a sql probing tool for Python that provides insight into specific parts of sql queries.
`sqlpt` is to a sql query as a multimeter is to a circuit board.

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

Another quick example before a more comprehensive description--let's probe a scalar subquery in the select claus:

```python
>>> sql_str = '''
        select subject,
                course_number,
                (select name from term where section.term_id = term.id) name
           from section
'''

>>> query = Query(sql_str)

>>> query.select_clause.fields[2].query.crop().count()
2
```

# Reasoning

Accurate and well-performing sql queries take careful construction. Having a good understanding of the tables, joins, and filters is essential to forming such queries. `sqlpt` provides tools to inspect areas of sql queries to make more informed design decisions.

These tools utilize sql parsing (`python-sqlparse`) but also provide the probing functionality described above.

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
- [ ] Count expected rows in a delete statement
- [x] Identify filters in join and where clauses
- [x] Check table granularity
- [x] Check if query is leaf query
- [x] Ignore dangling parameters
- [x] Locate columns in expressions
- [ ] Generate a diff between sql queries

## Modifying

- [ ] Add/remove select-clause field
- [ ] Add/remove from-clause join
- [ ] Add/remove where-clause filter
- [x] Crop where-clause filter
- [x] Convert left join without where-clause filter to scalar subquery in select clause
- [x] Parameterize query with dangling comparison terms
- [ ] Convert select statement to update statement

# Future areas of improvement

## Code
- [ ] Underscore methods and where to locate methods (move some to service)
- [ ] Distinguish between s_str and sql_str everywhere (s_str being a snippet? and sql_str a full query sql)
- [ ] Document all different ways to construct each clause