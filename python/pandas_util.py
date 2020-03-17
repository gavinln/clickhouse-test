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


def shape(df):
    row, col = df.shape
    return '{} rows, {} cols'.format(intword(row), intword(col))


def dtype_counts_frame(df):
    return df.dtypes.value_counts(
        ).rename_axis('dtype').to_frame('counts').reset_index()


def dtype_counts(df):
    ' number of each dtype columns'
    return df.dtypes.value_counts()


def na_counts(df):
    ''' number of na (not-available) values in each column

    >>> df = pd.DataFrame({'a': [0, 1, None], 'b': [1, 2, None], 'c': 'xyz'})
    >>> na_counts(df)
    a    1
    b    1
    c    0
    dtype: int64
    '''
    return df.isna().sum()


def na_fraction(df):
    ''' fraction of na (not-available) values in each column

    >>> df = pd.DataFrame({'a': [0, 1, None], 'b': [1, 2, None], 'c': 'xyz'})
    >>> na_fraction(df) * 100
    a    33.333333
    b    33.333333
    c     0.000000
    dtype: float64
    '''
    return df.isna().sum()/df.shape[0]


def na_column_counts(df):
    ''' number of columns with each count of na (not-available) values

    >>> data = {'a': [0, 1, None], 'b': [1, 1, None], 'c': list('xyz') }
    >>> na_column_counts(pd.DataFrame(data))
    0    1
    1    2
    dtype: int64
    '''
    return na_counts(df).value_counts().sort_index()


def na_column_fraction(df):
    ''' fraction of columns with each count of na (not-available) values

    >>> data = {'a': [0, 1, None], 'b': [1, 1, None], 'c': list('xyz') }
    >>> na_column_fraction(pd.DataFrame(data))
    0    0.333333
    1    0.666667
    dtype: float64
    '''
    return na_counts(df).value_counts().sort_index()/df.shape[0]


def unique_counts(df):
    ''' number of na (not-available) values in each column

    >>> data = {'a': [0, 1, None], 'b': [1, 1, None], 'c': list('xyz') }
    >>> unique_counts(pd.DataFrame(data))
    a    2
    b    1
    c    3
    dtype: int64
    '''
    return df.nunique()


def unique_fraction(df):
    ''' fraction of na (not-available) values in each column

    >>> data = {'a': [0, 1, None], 'b': [1, 1, None], 'c': list('xyz') }
    >>> unique_fraction(pd.DataFrame(data))
    a    0.666667
    b    0.333333
    c    1.000000
    dtype: float64
    '''
    return unique_counts(df)/df.shape[0]


def min_max_values(df):
    return df.select_dtypes(exclude=['category', 'string']).apply(
        lambda srs: [srs.min(), srs.max()],
        axis='index', result_type='expand').transpose().rename(
        columns={0: 'min_value', 1: 'max_value'})


def select_ints(df):
    return df.select_dtypes(include=np.int_)


def select_floats(df):
    return df.select_dtypes(include=np.float_)


def mem_usage(df):
    return intword(df.memory_usage(deep=True).sum())


def fraction_part_abs_max(srs):
    return (srs - srs.round()).abs().max()


def main():
    print('pandas utilities')
    data_file = (
        SCRIPT_DIR / '..' /
        'clickhouse' / 'airline-data' / '2008_cleaned.gzip.parq').resolve()
    data_file = (
        SCRIPT_DIR / '..' / 'temp.parq').resolve()
    if not data_file.is_file():
        print('missing file', data_file)
    print('using file', data_file)

    with timed():
        table = pq.read_table(data_file)
        df = table.to_pandas()
        print('reading parquet file: size with numpy types',
              intword(df.memory_usage(deep=True).sum()))

    print(df.head())
    embed()


if __name__ == '__main__':
    main()
