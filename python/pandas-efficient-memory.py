'''
Handling large data in Pandas: 10 million to 100 million rows.

Pandas is built on numpy. Numpy can work with numeric data like integers and
floats very efficiently. Pandas can store a wider range of objects types than
numpy including numeric data, strings, dates and times. However the non-numpy
types are not stored very efficiently.

In the latest versions of pandas (around 1.0) more efficient pandas types were
introduced. By default pandas reads data using a combination of efficient
numeric numpy types and inefficient pandas object types. Use the convert_dtypes
function to convert default pandas data frame types to more efficent types.
'''
import logging
from pathlib import Path
from contextlib import contextmanager
from time import time
from datetime import datetime as dt

import textwrap

import pandas as pd

import pyarrow.parquet as pq

from humanize import intword
# from IPython import embed


logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


SCRIPT_DIR = Path(__file__).parent.resolve()


@contextmanager
def timed():
    'Simple timer context manager, implemented using a generator function'
    start = time()
    print("Starting at {:%H:%M:%S}".format(dt.fromtimestamp(start)))

    yield

    end = time()
    print("Ending at {:%H:%M:%S} (total: {:.2f} seconds)".format(
        dt.fromtimestamp(end), end - start))


def write_parquet(df, parq_file):
    df.to_parquet(parq_file)


def write_parquet_gzip(df, parq_file):
    df.to_parquet(parq_file, compression='gzip')


def print_parq_file_info(parq_file):
    parq_file = pq.ParquetFile(parq_file)
    print(textwrap.indent(str(parq_file.metadata), prefix='\t'))
    print(textwrap.indent(str(parq_file.schema), prefix='\t'))


def main() -> None:
    data_file = (
        SCRIPT_DIR / '1988_cleaned.parq').resolve()
    if not data_file.is_file():
        print('missing file', data_file)
    print('using file', data_file)

    with timed():
        print_parq_file_info(data_file)
        table = pq.read_table(data_file)
        # df = pd.read_parquet(data_file)
        df = table.to_pandas()
        print('reading parquet file: size with numpy types',
              intword(df.memory_usage(deep=True).sum()))

    print('data size: {}'.format(df.shape))

    df2 = df.convert_dtypes()
    print('size with pandas types', intword(df2.memory_usage(deep=True).sum()))

    min_max_df3 = df2.select_dtypes(exclude='string').apply(
        lambda srs: [srs.min(), srs.max()],
        axis='index', result_type='expand').transpose()

    Int16_cols = min_max_df3.index
    col_types = dict(zip(Int16_cols, ['Int16'] * len(Int16_cols)))

    df4 = df2.astype(col_types)
    print('size with pandas custom types',
          intword(df4.memory_usage(deep=True).sum()))

    unique_df5 = df4.select_dtypes(include='string').nunique()
    cat_columns = unique_df5[unique_df5 < 500].index

    col_types = dict(zip(cat_columns, ['category'] * len(cat_columns)))
    df6 = df4.astype(col_types)
    print('size with pandas custom and category types',
          intword(df6.memory_usage(deep=True).sum()))

    temp_parq = (SCRIPT_DIR / 'temp' / 'temp.parq').resolve()
    write_parquet_gzip(df6, temp_parq)

    with timed():
        print_parq_file_info(temp_parq)
        df6 = pd.read_parquet(temp_parq)
        print('reading size with pandas custom and category types from file',
              intword(df6.memory_usage(deep=True).sum()))


if __name__ == '__main__':
    main()
