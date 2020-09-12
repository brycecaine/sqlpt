import os


def list_abs_paths(path):
    for dirpath, _, filenames in os.walk(path):
        for f in filenames:
            yield os.path.abspath(os.path.join(dirpath, f))
