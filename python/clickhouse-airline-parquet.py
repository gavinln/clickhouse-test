'''
The airline data set is used which has about 123 million rows from 1987 to 2008
'''
import logging
from pathlib import Path
import time
import sys
from distutils.spawn import find_executable

from subprocess import check_output

import pandas as pd

import sqlalchemy

import fire


logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


SCRIPT_DIR = Path(__file__).parent.resolve()


def write_parquet(df, parq_file):
    df.to_parquet(parq_file)


def write_parquet_gzip(df, parq_file):
    df.to_parquet(parq_file, compression='gzip')


def get_parquet_column_types(parq_files):
    dtypes = []
    for parq_file in sorted(parq_files):
        print('processing {}'.format(parq_file.name))
        df = pd.read_parquet(parq_file)
        dtypes.append(df.dtypes)

    dtypes_frame = pd.concat(dtypes, axis=1)

    def combine_types(srs):
        return set(srs.to_list())

    dtypes_srs = dtypes_frame.apply(combine_types, axis=1)
    return dtypes_srs


def get_clickhouse_engine():
    host = '10.0.0.2'
    host = '127.0.0.1'
    engine = sqlalchemy.create_engine(
        'clickhouse://default:@{}:8123/default'.format(host))
    return engine


def execute_sql(sql):
    engine = get_clickhouse_engine()
    with engine.begin() as connection:
        result = connection.execute(sql)
        for row in result:
            print(row)


def list_database_tables(engine: sqlalchemy.engine.base.Engine, database: str) -> None:
    if database is None or len(database) == 0:
        sys.exit('Invalid database name')

    with engine.begin() as connection:
        sql = 'show tables from {}' .format(database)
        breakpoint()
        result = connection.execute(sql)
        for row in result:
            print(row)



def list_system_tables() -> None:
    ' test db connectivity by listing tables '
    engine = get_clickhouse_engine()
    list_database_tables(engine, 'system')


def list_default_tables() -> None:
    ' list tables in default database '
    engine = get_clickhouse_engine()
    list_database_tables(engine, 'default')


def check_clickhouse_client():
    ' return False if clickhouse-client is not available '
    prg = find_executable('clickhouse-client')
    if prg is None:
        sys.exit('Cannot find clickhouse-client. Is it in the PATH?')
    output = check_output('{} --version'.format(prg), shell=True)
    print('Found {}'.format(output.decode('unicode_escape')))


def create_flight_table():
    sql = '''
        create table if not exists flight (
            Year              Int16,
            Month             Int8,
            DayofMonth        Int16,
            DayOfWeek         Int8,
            DepTime           Nullable(Int16),
            CRSDepTime        Int16,
            ArrTime           Nullable(Int16),
            CRSArrTime        Int16,
            UniqueCarrier     String,
            FlightNum         Int32,
            TailNum           Nullable(String),
            ActualElapsedTime Nullable(Int32),
            CRSElapsedTime    Nullable(Int32),
            AirTime           Nullable(Int32),
            ArrDelay          Nullable(Int32),
            DepDelay          Nullable(Int32),
            Origin            String,
            Dest              String,
            Distance          Nullable(Int32),
            TaxiIn            Nullable(Int32),
            TaxiOut           Nullable(Int32),
            Cancelled         Int8,
            CancellationCode  Nullable(String),
            Diverted          Int8,
            CarrierDelay      Nullable(Int32),
            WeatherDelay      Nullable(Int32),
            NASDelay          Nullable(Int32),
            SecurityDelay     Nullable(Int32),
            LateAircraftDelay Nullable(Int32)
        ) ENGINE = MergeTree
        Order by Year
    '''
    execute_sql(sql)


def create_flight_view():
    sql = '''
        create materialized view flight_view
        engine = AggregatingMergeTree() ORDER BY (Origin, Year, Month)
        as select
            Origin, Year, Month,
            avgState(DepDelay) as avg_DepDelay,
            countState(DepDelay) as count_DepDelay
        from flight
        group by Origin, Year, Month
    '''
    execute_sql(sql)


def query_flight_table():
    ' 5.41 s '
    sql = '''
        select Origin, Year, Month,
            avg(DepDelay),
            count(DepDelay)
        from flight
        group by Origin, Year, Month
        having count(DepDelay) > 35000
    '''
    execute_sql(sql)


def query_flight_view():
    ' 0.72s '
    sql = '''
        select Origin, Year, Month,
            avgMerge(avg_DepDelay),
            countMerge(count_DepDelay)
        from flight_view
        group by Origin, Year, Month
        having countMerge(count_DepDelay) > 35000
    '''
    execute_sql(sql)


def create_flight_tables():
    ' create flight table and materialized view '
    create_flight_table()

    create_flight_view()


def load_flight_data() -> None:
    '''
    Load flight data

    Loading data without AggregatingMergeTree: 3m18s
    Loading data with AggregatingMergeTree: 3m18s
    '''
    parq_file_dir = (
        SCRIPT_DIR / '..' / 'clickhouse' / 'airline-data').resolve()

    if not parq_file_dir.exists():
        sys.exit(f'Parquet file directory {parq_file_dir} does not exist')


    parq_files = list(parq_file_dir.glob('*_cleaned.gzip.parq'))
    parq_file_count = len(parq_files)

    print(f'There are {parq_file_count} files')

    check_clickhouse_client()

    for idx, parq_file in enumerate(sorted(list(parq_files))):
        print('processing {}/{} {}'.format(idx + 1, parq_file_count, parq_file))
        prg = 'clickhouse-client --query="INSERT INTO flight FORMAT Parquet"'
        output = check_output("cat {} | {}".format(parq_file, prg),
                              shell=True)
        print(output.decode('utf-8').strip())

    start_time = time.time()
    query_flight_table()
    elapsed = time.time() - start_time
    print('Elapsed = {:,.2f} seconds'.format(elapsed))

    start_time = time.time()
    query_flight_view()
    elapsed = time.time() - start_time
    print('Elapsed = {:,.2f} seconds'.format(elapsed))

    # dtypes_srs = get_parquet_column_types(parq_files)
    # print(dtypes_srs)


def main():
    fire.Fire({
        'list-system-tables': list_system_tables,
        'list-default-tables': list_default_tables,
        'create-flight-tables': create_flight_tables,
        'load-flight-data': load_flight_data
    })


if __name__ == '__main__':
    main()
