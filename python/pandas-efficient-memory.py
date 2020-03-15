'''

Handling large data in Pandas 10 million to 100 million Pandas is built on
numpy. Numpy can work with numeric data like integers and floats very
efficiently. Pandas can store a much wider range of objects than numpy which
include numeric data, strings, dates and times. However the non-numpy types are
not stored very efficiently.

In the latest versions of pandas (around 1.0) more efficient pandas types were
introduced. By default pandas reads data using a combination of efficient
numeric numpy types and inefficient pandas object types. Use the convert_dtypes
function to convert default pandas data frame types to more efficent types.


In IPython use
In [1]: %load_ext autoreload
In [2]: %autoreload 2
'''
import logging
from pathlib import Path
from contextlib import contextmanager
from time import time
from datetime import datetime as dt

import numpy as np
import pandas as pd

import pyarrow.parquet as pq
import pyarrow as pa

from humanize import intword
from IPython import embed


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


# @pd.api.extensions.register_dataframe_accessor("meta")
# class MetaAccessor:
#     def __init__(self, pandas_obj):
#         self._validate(pandas_obj)
#         self._obj = pandas_obj
#     @staticmethod
#     def _validate(obj):
#         # validate pandas object
#         pass
#     @property
#     def shape_(self):
#         row, col = self._obj.shape
#         return '{} rows, {} cols'.format(intword(row), intword(col))
#     def plot_(self):
#         pass


def write_parquet(df, parq_file):
    df.to_parquet(parq_file)


def write_parquet_gzip(df, parq_file):
    df.to_parquet(parq_file, compression='gzip')


def shape(df):
    row, col = df.shape
    return '{} rows, {} cols'.format(intword(row), intword(col))


def dtype_counts_frame(df):
    return df.dtypes.value_counts(
        ).rename_axis('dtype').to_frame('counts').reset_index()


def dtype_counts(df):
    return df.dtypes.value_counts()


def na_counts(df):
    return df.isna().sum()


def na_column_counts(df):
    return na_counts(df).value_counts().sort_index()


def select_ints(df):
    return df.select_dtypes(include=np.int_)


def select_floats(df):
    return df.select_dtypes(include=np.float_)


def mem_usage(df):
    return intword(df.memory_usage(deep=True).sum())


def fraction_part_abs_max(srs):
    return (srs - srs.round()).abs().max()


def boolean_series_null():
    """
    >>> # does not support null values, None treated as False
    >>> srs = pd.Series([True, False, None], dtype='bool')
    >>> srs.value_counts()
    False    2
    True     1
    dtype: int64
    >>> # supports null values
    >>> srs = pd.Series([True, False, None], dtype='boolean')
    >>> srs.value_counts()
    False    1
    True     1
    dtype: Int64
    >>> # None is treated as a object
    >>> srs = pd.Series([True, False, None])
    >>> srs.value_counts()
    True     1
    False    1
    dtype: int64
    """
    pass


def boolean_memory_series():
    """
    >>> srs = pd.Series([True, False, None] * 4_000)
    >>> srs.memory_usage(deep=True)
    368128
    >>> srs = pd.Series([True, False] * 6_000, dtype='bool')
    >>> srs.memory_usage(deep=True)
    12128
    >>> srs = pd.Series([True, False, None] * 4_000, dtype='boolean')
    >>> srs.memory_usage(deep=True)
    24128
    """
    pass


def int_null_series():
    """
    >>> # raise NoneType error
    >>> # srs = pd.Series([1, 2, None], dtype='int64')
    >>> srs = pd.Series([1, 2, None], dtype='Int64')
    >>> srs.value_counts()
    1    1
    2    1
    dtype: Int64
    >>> srs = pd.Series([1, 2, None])
    >>> srs.value_counts()
    2.0    1
    1.0    1
    dtype: int64
    """
    pass


def int_memory_series():
    """
    >>> # memory used with null values
    >>> srs = pd.Series([1, 2, None] * 4_000, dtype='Int64')
    >>> srs.memory_usage(deep=True), srs.dtypes
    (108128, Int64Dtype())
    >>> srs = pd.Series([1, 2, None] * 4_000)
    >>> srs.memory_usage(deep=True), srs.dtypes
    (96128, dtype('float64'))
    >>> srs = pd.Series([1, 2, None] * 4_000, dtype='float16')
    >>> srs.memory_usage(deep=True), srs.dtypes
    (24128, dtype('float16'))
    >>> srs = pd.Series([1, 2, None] * 4_000, dtype='Int8')
    >>> srs.memory_usage(deep=True), srs.dtypes
    (24128, Int8Dtype())
    >>> # memory used without null values
    >>> srs = pd.Series([1, 2] * 6_000, dtype='int64')
    >>> srs.memory_usage(deep=True), srs.dtypes
    (96128, dtype('int64'))
    >>> srs = pd.Series([1, 2] * 6_000, dtype='int8')
    >>> srs.memory_usage(deep=True), srs.dtypes
    (12128, dtype('int8'))
    """
    pass


# TODO: See infer_objects
# TODO: Add code separately in addition to documentation
# TODO: Put simplest option first: no None, with None
# TODO: Put numpy types first then pandas types

def string_null_series():
    srs = pd.Series(['A', 'B'] * 6_000)
    srs.memory_usage(deep=True), srs.dtypes  # (792128, dtype('O'))
    srs = pd.Series(['A', 'B'] * 6_000, dtype='string')
    srs.memory_usage(deep=True), srs.dtypes  # (96128, StringDtype)
    srs = pd.Series(['A', 'B', None] * 4_000)
    srs.memory_usage(deep=True), srs.dtypes  # (624128, dtype('O'))
    srs = pd.Series(['A', 'B', None] * 4_000, dtype='string')
    srs.memory_usage(deep=True), srs.dtypes  # (96128, StringDtype)


def print_parq_file_info(parq_file):
    parq_file = pq.ParquetFile(parq_file)
    print(parq_file.metadata)
    print(parq_file.schema)


def temp():
    # process floats
    # if int with null convert to Int
    # check bounds
    # convert to smaller float type

    # process ints
    # convert to smaller type

    # process strings
    # count unique values
    # convert to enumeration type

    # process boolean
    # convert to

    # process dates

    # min and max for each column
    # df2.apply(lambda srs: [srs.min(), srs.max()], axis='index')
    # df3 = df2.apply(
    #     lambda srs: [srs.min(), srs.max()],
    #     axis='index', result_type='expand').transpose()
    pass


def main() -> None:
    print('in main')
    data_file = (
        SCRIPT_DIR / '..' /
        'clickhouse' / 'airline-data' / '2008_cleaned.gzip.parq').resolve()
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

    with timed():
        csv_data_file = (SCRIPT_DIR / '..' / '2008_cleaned.csv').resolve()
        print('writing csv file', csv_data_file)
        df.to_csv(csv_data_file, index=False)

    with timed():
        parq_data_file = (SCRIPT_DIR / '..' / '2008_cleaned.parq').resolve()
        print('writing parq file', parq_data_file)
        write_parquet(df, parq_data_file)
        print_parq_file_info(parq_data_file)

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

    temp_parq = (SCRIPT_DIR / '..' / 'temp.parq').resolve()
    write_parquet(df6, temp_parq)

    with timed():
        print_parq_file_info(temp_parq)
        df6 = pd.read_parquet(temp_parq)
        print('reading size with pandas custom and category types from file',
              intword(df6.memory_usage(deep=True).sum()))


if __name__ == '__main__':
    main()
