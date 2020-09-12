import os

import click

import sql
import sqlpt


@click.command()
@click.option('--path', help='Path of folder or file containing sql queries')
def process(path=None):
    """Processes sql files in path and generates query objects"""

    queries = []

    db_str = 'sqlite:///college.db'

    for filename in sqlpt.list_abs_paths(path):
        click.echo(filename)

        with open(filename, 'rt', encoding='utf-8') as sql_file:
            sql_str = sqlpt.get_sql_str(sql_file)

        # sql_statement = sqlpt.get_sql_statement(sql_str)
        incoming_query = sql.Query(sql_str, db_str)

        click.echo(incoming_query.format())
        click.echo(incoming_query.describe())
        click.echo(incoming_query.head())

        incoming_query.name = click.prompt('Query name', type=str)

        similar_processing_choice = 2

        for query in queries:
            # if incoming_query.from_clause == query.from_clause:
            click.echo('The incoming query is similar to this previously '
                       'processed query.')
            click.echo('1. Fuse both queries into one')
            click.echo('2. Add incoming query as is')
            click.echo('3. Skip incoming query')

            similar_processing_choice = click.prompt(
                'Please select one',
                type=click.Choice(['1', '2', '3']),
                show_default=False,
            )

            print(similar_processing_choice)

        if similar_processing_choice == 1:
            # fused_query = fuse(incoming_query, query)
            # queries.append(fused_query)
            print(f'fusing {query}')

        elif similar_processing_choice == 2:
            queries.append(incoming_query)


def main(path=None):
    process(path)


if __name__ == '__main__':
    main()
