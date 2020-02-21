import logging
from pathlib import Path

from subprocess import Popen
from subprocess import PIPE
from subprocess import STDOUT

import pandas as pd

import sqlalchemy as sa


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

'''
select Origin, Year, Month, avg(DepDelay), count(*)
from flight
group by Origin, Year, Month
having count(*) > 35000;
'''

def create_flight_table():
    sql = '''
        create table flight (
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
        ) ENGINE = Log
    '''
    engine = sa.create_engine('clickhouse://default@10.0.0.2:8123/default')
    with engine.begin() as connection:
        result = connection.execute(sql)
        print(result.fetchall())


def main() -> None:
    parq_file_dir = (
        SCRIPT_DIR / '..' / 'clickhouse' / 'airline-data').resolve()
    parq_files = parq_file_dir.glob('*_cleaned.gzip.parq')

    # dtypes_srs = get_parquet_column_types(parq_files)
    # print(dtypes_srs)

    create_flight_table()

    # with Popen(["date", "-h"], stdout=PIPE) as proc:
    #     result = proc.stdout.read().decode('utf-8')
    #     print(result)

if __name__ == '__main__':
    main()
