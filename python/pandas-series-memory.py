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

import numpy as np
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
    >>> srs = pd.Series([True, False] * 6_000, dtype='bool')
    >>> srs.memory_usage(deep=True)
    12128
    >>> srs = pd.Series([True, False, None] * 4_000)
    >>> srs.memory_usage(deep=True)
    368128
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
    >>> # memory used without null values
    >>> srs = pd.Series([1, 2] * 6_000, dtype='int64')
    >>> srs.memory_usage(deep=True), srs.dtypes
    (96128, dtype('int64'))
    >>> srs = pd.Series([1, 2] * 6_000, dtype='int8')
    >>> srs.memory_usage(deep=True), srs.dtypes
    (12128, dtype('int8'))
    >>> # memory used with null values
    >>> srs = pd.Series([1, 2, None] * 4_000)
    >>> srs.memory_usage(deep=True), srs.dtypes
    (96128, dtype('float64'))
    >>> srs = pd.Series([1, 2, None] * 4_000, dtype='float16')
    >>> srs.memory_usage(deep=True), srs.dtypes
    (24128, dtype('float16'))
    >>> srs = pd.Series([1, 2, None] * 4_000, dtype='Int64')
    >>> srs.memory_usage(deep=True), srs.dtypes
    (108128, Int64Dtype())
    >>> srs = pd.Series([1, 2, None] * 4_000, dtype='Int8')
    >>> srs.memory_usage(deep=True), srs.dtypes
    (24128, Int8Dtype())
    """
    pass


def string_null_series():
    srs = pd.Series(['A', 'B'] * 6_000)
    srs.memory_usage(deep=True), srs.dtypes  # (792128, dtype('O'))
    srs = pd.Series(['A', 'B'] * 6_000, dtype='string')
    srs.memory_usage(deep=True), srs.dtypes  # (96128, StringDtype)
    srs = pd.Series(['A', 'B', None] * 4_000)
    srs.memory_usage(deep=True), srs.dtypes  # (624128, dtype('O'))
    srs = pd.Series(['A', 'B', None] * 4_000, dtype='string')
    srs.memory_usage(deep=True), srs.dtypes  # (96128, StringDtype)


# TODO: See infer_objects
# TODO: Add code separately in addition to documentation
# TODO: Put simplest option first: no None, with None
# TODO: Put numpy types first then pandas types


def print_parq_file_info(parq_file):
    parq_file = pq.ParquetFile(parq_file)
    print(parq_file.metadata)
    print(parq_file.schema)


def main():
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


if __name__ == '__main__':
    main()
