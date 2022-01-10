"""
cache dataframes using duckdb
"""
import pathlib
import logging
import random
import time

import numpy as np
import pandas as pd
import duckdb


SCRIPT_DIR = pathlib.Path(__file__).parent.resolve()
log = logging.getLogger(__file__)


def get_random_table_name():
    return 'table_{}'.format(random.randrange(int(1e5), int(1e6)))


def get_df(conn, name: str):
    "get duckdb table as a dataframe"
    sql = f'select * from {name}'
    return conn.execute(sql).fetchdf()


def list_tables(conn):
    'get list of tables in duckdb'
    return [table_tuple[0] for table_tuple in conn.execute(
        "PRAGMA show_tables").fetchall()]


def has_table(conn, name: str):
    'returns true if table called name in duckdb'
    return name in list_tables(conn)


def get_new_table_name(conn):
    'get new name for table which does not exist'
    while True:
        table_name = get_random_table_name()
        if not has_table(conn, table_name):
            return table_name


def _create_table_from_df2(conn, df, register_name, name):
    'create duckdb table name from dataframe df'
    # can register a dataframe as the same name multiple times
    # ideally should use a new name when registering a table
    # _create_table_from_df function is superior to this one
    conn.register(register_name, df)
    sql = "create table {} as select * from {}".format(name, register_name)
    conn.execute(sql)
    conn.unregister(register_name)


def _create_table_from_df(conn, df, name):
    'create duckdb table name from dataframe df'
    rel = conn.from_df(df)
    rel.create(name)


def save_df(conn, df, name):
    "save dataframe to a new table in duckdb"
    _create_table_from_df(conn, df, name)


def get_example_name_value_df(rows=10):
    assert rows % 2 == 0, 'rows should be an even number'
    # names = list('ab' * (rows // 2))
    # random.shuffle(names)
    # 97, 98 for lowercase a, b in ascii
    chr_number_list = np.random.randint(97, 99, rows)
    names = map(chr, chr_number_list.tolist())
    values = np.random.randint(0, 100, rows)
    return pd.DataFrame({'name': names, 'value': values})


def timer_func(func):
    # This function shows the execution time of the function
    def wrap_func(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        elapsed = time.time() - start
        print(
            f'Function {func.__name__!r} executed in {elapsed:.4f}s',
            flush=True)
        return result
    return wrap_func


def main() -> None:
    conn = duckdb.connect(database=":memory:", read_only=False)
    df = pd.DataFrame({
        'name': list(['a', 'b', 'a']),
        'value': range(1, 4)
    })
    save_df(conn, df, 'table1')
    save_df(conn, df, 'table2')
    print('tables in duckdb', list_tables(conn))
    print('table1 contents')
    print(get_df(conn, 'table1'))

    print('Generating and saving dataframe ...')
    start = time.time()
    example_df = get_example_name_value_df(rows=400_000_000)
    # example_df = get_example_name_value_df(rows=10)
    print('Time to generate dataframe with {:,} rows: {:.4f}s'.format(
          example_df.shape[0], time.time() - start))

    start = time.time()
    save_df(conn, example_df, 'table3')
    print('Time to save dataframe with {:,} rows: {:.4f}s'.format(
          example_df.shape[0], time.time() - start))

    print('It takes about 2 minutes to group and sum data. Please wait...')

    @timer_func
    def get_pandas_group_sum():
        '47 seconds for 400 million rows'
        return example_df.groupby('name').sum()

    @timer_func
    def get_duckdb_group_sum():
        '''
            33 seconds for 400 million rows (includes storing in duckdb)
            3 seconds for 400 million rows (without storing in duckdb)
        '''
        return conn.execute(
            'select name, sum(value) from table3 group by name').fetchdf()

    result1 = get_pandas_group_sum()
    print(result1)

    result2 = get_duckdb_group_sum()
    print(result2)


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARN)
    main()
