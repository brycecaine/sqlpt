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

# Probing

## Row counts

### Counting rows in a query

```python
query.count()
```

### Counting rows in underlying datasets

```python
query.counts()
```

### Counting expected rows in an update statement

### Counting expected rows in a delete statement

## Filters

### Identifying filters in join and where clauses

## Inspection

### Checking table granularity

### Checking if query is leaf query

### Ignoring dangling parameters

### Locating invalid columns in expressions

### Generating a diff between sql queries

# Modifying

## Select clause

### Adding a field

### Removing a field

### Converting to an update statement

## From clause

### Adding a join

### Removing a join

### Converting a left join to a scalar subquery

## Where clause

### Adding a filter

### Removing a filter

### Cropping a filter

### Parameterizing dangling comparison terms

