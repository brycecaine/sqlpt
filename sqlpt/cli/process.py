import click

from sqlpt import cli, sql


@click.command()
@click.option('--db-str', help='Database connection string')
@click.option('--input-dir', help='Input path containing sql queries')
@click.option('--output-dir', help='Output path containing sql queries')
@click.option('--dry-run', is_flag=True, help='Do not output sql files')
def process(db_str, input_dir, output_dir, dry_run):
    """Processes sql files in input_path and generates query output"""

    queries = []

    filenames = cli.service.list_abs_paths(input_dir)

    for i, filename in enumerate(filenames, start=1):
        click.clear()
        click.echo('=' * 79)
        click.echo(f'SQL File {i}: {filename}')

        with open(filename, 'rt', encoding='utf-8') as sql_file:
            sql_str = sql_file.read()

        incoming_query = sql.Query(sql_str, db_str)

        formatted_sql = incoming_query.format_sql()
        data_description = incoming_query.dataframe.describe()
        data_sample = incoming_query.dataframe.head()

        click.echo('-' * 79)
        click.echo(formatted_sql)
        click.echo('-' * 79)
        click.echo(data_description)
        click.echo('-' * 79)
        click.echo(data_sample)

        click.echo('-' * 79)
        click.echo('1. Process incoming query')
        click.echo('2. Skip incoming query')

        process_incoming_choice = click.prompt(
            'Please select one',
            type=click.Choice(['1', '2']),
            default=1,
        )

        if process_incoming_choice == 1:
            query_name = click.prompt('Query name', type=str)

            incoming_query.name = query_name

            # ---------------------------------------------------------------------
            # Similar query
            similar_processing_choice = None

            for query in queries:
                if incoming_query.from_clause == query.from_clause:
                    click.echo('-' * 79)
                    click.echo('The incoming query is similar to this '
                               'previously processed query.')
                    click.echo('1. Add incoming query as is')
                    click.echo('2. Fuse both queries into one')

                    similar_processing_choice = click.prompt(
                        'Please select one',
                        type=click.Choice(['1', '2']),
                        default=1,
                    )

            if similar_processing_choice == 2:
                # fused_query = fuse(incoming_query, query)
                # queries.append(fused_query)
                print(f'fusing {query}')

            queries.append(incoming_query)

            if not dry_run:
                output_sql_path = f'{output_dir}/sql/{query_name}.sql'
                incoming_query.output_sql_file(output_sql_path)

                output_data_path = f'{output_dir}/data/{query_name}.csv'
                incoming_query.output_data_file(output_data_path)


if __name__ == '__main__':
    process(None, None, None, None)
