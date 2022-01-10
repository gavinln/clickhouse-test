"""
cache dataframes using duckdb
"""
import pathlib
import logging
import random

import pandas as pd
import duckdb


SCRIPT_DIR = pathlib.Path(__file__).parent.resolve()
log = logging.getLogger(__file__)


def get_db_conn():
    "get duckdb connection"
    conn = duckdb.connect(database=":memory:", read_only=False)
    return conn

'''
rel = conn.from_df(df)
rel.alias
rel.columns
rel.types
get_tables(conn)
rel.create('temp_table')
get_tables(conn)
conn.execute('select * from temp_table')
conn.execute('select * from temp_table').fetchdf()
conn.execute('select * from temp_table').df()
'''


def get_random_table_name():
    return 'table_{}'.format(random.randrange(int(1e5), int(1e6)))


def get_df(conn, name: str):
    "get duckdb table as a dataframe"
    sql = f'select * from {name}'
    return conn.execute(sql).fetchdf()


def get_tables(conn):
    'get list of tables in duckdb'
    return [table_tuple[0] for table_tuple in conn.execute(
        "PRAGMA show_tables").fetchall()]


def get_new_table_name(conn):
    'get new name for table which does not exist'
    while True:
        table_name = get_random_table_name()
        if table_name not in get_tables(conn):
            return table_name


def save_df(conn, df, name):
    "save dataframe to a duckdb"
    temp_table = get_new_table_name(conn)
    conn.register(temp_table, df)
    sql = "create table {} as select * from {}".format(
        name, temp_table)
    conn.execute(sql)
    conn.unregister(temp_table)


def main() -> None:
    conn = get_db_conn()
    df = pd.DataFrame([[1, 2], [4, 4]], columns=list('ab'))
    save_df(conn, df, 'table1')
    save_df(conn, df, 'table2')
    print(get_tables(conn))
    print(get_df(conn, 'table1'))
    breakpoint()


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARN)
    main()
