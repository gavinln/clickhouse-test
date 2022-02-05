'''
This program reads the Parquet file 1988_cleaned.parq with gzip compresions.
This file has freely available data for airline flights for the year 1988. It
then converts the data frame data types to more specialized types and saves it
to another parquet file which can be read much faster than the original file.

To run this file install Python libraries using the command:

pip install pyarrow
pip install pandas

Pandas is built on numpy. Numpy can work with numeric data like integers and
floats very efficiently. Pandas can store a wider range of objects types than
numpy including numeric data, strings, dates and times. However the non-numpy
types are not stored very efficiently.

In the latest versions of pandas (around 1.0) more efficient pandas types were
introduced. By default pandas reads data using a combination of efficient
numeric numpy types and inefficient pandas object types. Use the convert_dtypes
function to convert default pandas data frame types to more efficient types.
'''
import logging
from pathlib import Path
from contextlib import contextmanager
from time import time
from datetime import datetime as dt

import textwrap

import pandas as pd

import pyarrow.parquet as pq


log = logging.getLogger(__name__)


SCRIPT_DIR = Path(__file__).parent.resolve()


def intword(value):
    return f'{value:,d}'


@contextmanager
def timed():
    'Simple timer context manager, implemented using a generator function'
    start = time()
    print(f"Starting at {dt.fromtimestamp(start):%H:%M:%S}")

    yield

    end = time()
    print("Ending at {:%H:%M:%S} (total: {:.2f} seconds)".format(
        dt.fromtimestamp(end), end - start))


def write_parquet(df, parq_file):
    df.to_parquet(parq_file)


def write_parquet_gzip(df, parq_file):
    df.to_parquet(parq_file, compression='gzip')


def get_parq_file_info_str(parq_file):
    parq_file = pq.ParquetFile(parq_file)
    return textwrap.indent(str(parq_file.metadata), prefix='\t') + \
        textwrap.indent(str(parq_file.schema), prefix='\t')


def main() -> None:
    data_file = (
        SCRIPT_DIR / '1988_cleaned.parq').resolve()

    if not data_file.is_file():
        log.error('missing file', data_file)
        return

    log.debug('using file', data_file)

    with timed():
        log.debug(get_parq_file_info_str(data_file))
        table = pq.read_table(data_file)
        df = table.to_pandas()
        log.info('reading parquet file: size with numpy types %s',
                 intword(df.memory_usage(deep=True).sum()))

    log.info(f'data size: {df.shape}')

    df2 = df.convert_dtypes()  # converts to more efficient pandas data types
    log.info('size with pandas types %s',
             intword(df2.memory_usage(deep=True).sum()))

    min_max_df3 = df2.select_dtypes(exclude='string').apply(
        lambda srs: [srs.min(), srs.max()],
        axis='index', result_type='expand').transpose()

    Int16_cols = min_max_df3.index
    col_types = dict(zip(Int16_cols, ['Int16'] * len(Int16_cols)))

    df4 = df2.astype(col_types)
    log.info('size with pandas custom types %s',
             intword(df4.memory_usage(deep=True).sum()))

    unique_df5 = df4.select_dtypes(include='string').nunique()
    cat_columns = unique_df5[unique_df5 < 500].index

    col_types = dict(zip(cat_columns, ['category'] * len(cat_columns)))
    df6 = df4.astype(col_types)
    log.info('size with pandas custom and category types %s',
             intword(df6.memory_usage(deep=True).sum()))

    temp_parq = (SCRIPT_DIR / 'temp.parq').resolve()
    write_parquet_gzip(df6, temp_parq)

    with timed():
        log.debug(get_parq_file_info_str(temp_parq))
        df6 = pd.read_parquet(temp_parq)
        log.info(
            'reading size with pandas custom and category types from file %s',
            intword(df6.memory_usage(deep=True).sum()))


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    # logging.basicConfig(level=logging.INFO)
    main()
