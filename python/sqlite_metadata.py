import logging
import pathlib
import sqlite3
import difflib

import pandas as pd

from sqlalchemy.ext.automap import automap_base
from sqlalchemy import create_engine
from sqlalchemy import inspect

from IPython import embed

import fire

from sqlite_metadata_lib import query_yes_no


logging.basicConfig(level=logging.WARNING)
log = logging.getLogger(__name__)


SCRIPT_DIR = pathlib.Path(__file__).parent.resolve()


def get_sqlalchemy_base(db_info):
    log.info('retrieving tables from {}'.format(db_info))
    engine = create_engine(db_info)
    Base = automap_base()
    # reflect the tables
    Base.prepare(engine, reflect=True)
    return Base


def print_db_tables(sqlalchemy_base):
    table_names = sqlalchemy_base.classes.keys()
    log.info('retrieved {} tables'.format(len(table_names)))
    tables = ['{} {}'.format(
        idx, table) for idx, table in enumerate(table_names)]
    print('\n'.join(tables))


def display_model(model):
    print(model.__table__)
    inst = inspect(model)
    for attr, col in inst.mapper.column_attrs.items():
        print(repr(col.columns[0]))


def list_tables():
    ' show tables in the sqlite database '
    DB_FILE = get_db_file()
    db_info = 'sqlite:///' + str(DB_FILE)

    Base = get_sqlalchemy_base(db_info)
    print_db_tables(Base)

    for b_class in Base.classes:
        display_model(b_class)


def get_db_file():
    DB_FILE = SCRIPT_DIR / '..' / 'classic-models-db' / 'sqlite_cm.db'
    return DB_FILE.resolve()


def get_sql_df(sql):
    conn = sqlite3.connect(get_db_file())
    df = pd.read_sql_query(sql, conn)
    return df


def get_table_list():
    sql = '''
        select name
        from sqlite_master
        where type = 'table'
        order by tbl_name
    '''
    df = get_sql_df(sql)
    return df


def get_table_columns(table_name):
    # https://stackoverflow.com/questions/604939/how-can-i-get-the-list-of-a-columns-in-a-table-for-a-sqlite-database/

    sql = '''
    SELECT
        m.name AS table_name,
        p.cid AS id,
        p.name AS name,
        p.type AS type,
        p.pk AS is_pk,
        p.[notnull] AS is_not_null,
        p.dflt_value AS default_val
    FROM sqlite_master m
        LEFT OUTER JOIN pragma_table_info((m.name)) p
            ON m.name <> p.name
    WHERE m.type = 'table'
        and m.name = '{}'
    ORDER BY table_name, p.cid
    '''.format(table_name)
    df = get_sql_df(sql)
    return df


def get_table_names():
    df = get_table_list()
    return df.name.to_list()


def get_close_table_matches(table_name):
    return difflib.get_close_matches(table_name, get_table_names())


def get_table_columns_robust(name):
    df = get_table_columns(name)
    if df.empty:
        close_matches = get_close_table_matches(name)
        if len(close_matches) > 0:
            message = 'Did you mean {}?'.format(close_matches[0])
            response = query_yes_no(message)
            if response:
                df = get_table_columns(close_matches[0])
            else:
                df = pd.DataFrame()
    return df


def table_fn():
    df = get_table_list()
    print(df)


def column_fn(name, col_name=None):
    ''' get column details for a table
    '''
    if col_name is None:
        df = get_table_columns_robust(name)
        if not df.empty:
            cols = ['name', 'type', 'is_pk', 'is_not_null']
            print(df[cols])
    else:
        df = get_table_columns_robust(name)
        if not df.empty:
            cols = ['name', 'type', 'is_pk', 'is_not_null']
            print(df[cols])
            embed()


def main():
    fire.Fire({
        'list': table_fn,
        'details': column_fn
    })


if __name__ == "__main__":
    main()
