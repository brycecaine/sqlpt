import os


def list_abs_paths(path):
    for dirpath, _, filenames in os.walk(path):
        for f in filenames:
            yield os.path.abspath(os.path.join(dirpath, f))


def get_sql_str(sql_file):
    sql_str = sql_file.read()

    return sql_str
