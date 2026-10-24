from contextlib import contextmanager
from pathlib import Path

import duckdb


@contextmanager
def open_database(database, *, read_only=False):
    if isinstance(database, duckdb.DuckDBPyConnection):
        yield database
    else:
        with duckdb.connect(str(Path(database)), read_only=read_only) as connection:
            yield connection
