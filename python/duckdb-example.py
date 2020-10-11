import sys
from pathlib import Path

import pandas as pd

import fire
import duckdb

import pyarrow.parquet as pq

from IPython import embed

'''

-- cannot use MergeTree engine as all columns Nullable
CREATE TABLE default.flight2 (
    Year Nullable(Int16),
    Month Nullable(Int8),
    DayofMonth Nullable(Int16),
    DayOfWeek Nullable(Int8),
    DepTime Nullable(Int16),
    CRSDepTime Nullable(Int16),
    ArrTime Nullable(Int16),
    CRSArrTime Nullable(Int16),
    UniqueCarrier Nullable(String),
    FlightNum Nullable(Int32),
    TailNum Nullable(String),
    ActualElapsedTime Nullable(Int32),
    CRSElapsedTime Nullable(Int32),
    AirTime Nullable(Int32),
    ArrDelay Nullable(Int32),
    DepDelay Nullable(Int32),
    Origin Nullable(String),
    Dest Nullable(String),
    Distance Nullable(Int32),
    TaxiIn Nullable(Int32),
    TaxiOut Nullable(Int32),
    Cancelled Nullable(Int8),
    CancellationCode Nullable(String),
    Diverted Nullable(Int8),
    CarrierDelay Nullable(Int32),
    WeatherDelay Nullable(Int32),
    NASDelay Nullable(Int32),
    SecurityDelay Nullable(Int32),
    LateAircraftDelay Nullable(Int32)
) ENGINE = Log

'''


class DuckDB:

    def test(self):
        print('in test')
        duckdb_file = '1988-duck.db'
        cursor = duckdb.connect(duckdb_file)
        sql = '''
            create table flight as
            select * from parquet_scan('1988_cleaned.parq')
        '''
        result = cursor.execute(sql).fetchall()
        print(result)


def main():
    fire.Fire({
        'duck': DuckDB
    })


if __name__ == '__main__':
    main()

