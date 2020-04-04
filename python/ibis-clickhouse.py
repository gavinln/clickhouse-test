import pandas as pd
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker
from sqlalchemy import func

from IPython import embed


import ibis


def clickhouse_metadata(engine):
    # Use pandas to read from SqlAlchemy engine
    sql = 'select * from flight limit 100000'
    df = pd.read_sql(sql, engine)
    print(df.columns)

    metadata = sa.MetaData(bind=engine)
    metadata.reflect(only=['flight'])
    flight_tbl = metadata.tables['flight']

    # cannot access type for nullable types in Clickhouse
    for column in flight_tbl.columns:
        print(column.name)
        # print(column.type)  # does not work for nullable types in Clickhouse


def ibis_interface(engine):
    clickhouse_host = '127.0.0.1'
    conn = ibis.clickhouse.connect(
        host=clickhouse_host, port=9000, database='default')

    # Get metadata for flight table

    sql = '''
    select database, table, name, type
    from system.columns
    where database = 'default'
        and table = 'flight'
    '''
    df2 = pd.read_sql(sql, engine)
    print(df2.head())

    table = conn.table('flight')
    ibis.options.interactive = True

    # Get TailNum count and row count
    # print(table.TailNum.count())
    # print(table.count())
    print(table.Origin.value_counts())
    embed()


def main():
    # Create SqlAlchemy engine
    clickhouse_host = '127.0.0.1'
    ch_url = 'clickhouse://default@{}:8123/default'.format(
        clickhouse_host)
    engine = sa.create_engine(ch_url)
    # clickhouse_metadata(engine)
    ibis_interface(engine)


if __name__ == '__main__':
    main()
