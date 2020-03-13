'''
In IPython use
In [1]: %load_ext autoreload
In [2]: %autoreload 2
'''
import logging
from pathlib import Path

import numpy as np
import pandas as pd

from IPython import embed

from humanize import intword

import clickhouse_util as chu


logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


SCRIPT_DIR = Path(__file__).parent.resolve()


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


def boolean_null_series():
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


def main() -> None:
    print('in main')
    data_file = (SCRIPT_DIR / '..' / '2008_cleaned.gzip.parq').resolve()
    print(data_file)
    df = pd.read_parquet(data_file)
    print('numpy types size', intword(df.memory_usage(deep=True).sum()))

    df2 = df.convert_dtypes()
    print('pandas types size', intword(df2.memory_usage(deep=True).sum()))

    min_max_df3 = df2.select_dtypes(exclude='string').apply(
        lambda srs: [srs.min(), srs.max()],
        axis='index', result_type='expand').transpose()

    Int16_cols = min_max_df3.index
    col_types = dict(zip(Int16_cols, ['Int16'] * len(Int16_cols)))

    df4 = df2.astype(col_types)
    print('pandas compatct types size',
          intword(df4.memory_usage(deep=True).sum()))

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

    # embed()


if __name__ == '__main__':
    main()
