'''
parquet size pandas size  null     dtype
string test ==============================
   1,524,036  792,000,128 no null  object
   1,524,032   96,000,128 no null  string
   2,539,871  624,000,128 has null object
   2,539,867   96,000,128 has null string
boolean test ==============================
   1,500,178   12,000,128 no null  bool
   1,500,190   24,000,128 no null  boolean
   2,523,930  368,000,128 has null object
   2,523,955   24,000,128 has null boolean
float test ==============================
   1,524,208   96,000,128 no null  float64
   2,540,009   96,000,128 has null float64
datetime test ==============================
   1,524,204   96,000,128 no null  datetime64[ns]
   2,540,005   96,000,128 has null datetime64[ns]
int test ==============================
   1,524,208   96,000,128 no null  int64
   1,524,120   48,000,128 no null  int32
   1,524,120   24,000,128 no null  int16
   1,524,120   12,000,128 no null  int8
   2,540,009   96,000,128 has null float64
   2,540,009  108,000,128 has null Int64
   2,539,937   60,000,128 has null Int32
   2,539,937   36,000,128 has null Int16
   2,539,937   24,000,128 has null Int8
random int test ==============================
no dictionary encoding ====================
  96,007,228   96,000,128 no null  int64
  48,002,918   48,000,128 no null  int32
  48,002,918   24,000,128 no null  int16
  48,003,701  108,000,128 has null Int64
  24,001,530   60,000,128 has null Int32
  24,001,530   36,000,128 has null Int16
with dictionary encoding ====================
  22,765,730   96,000,128 no null  int64
  22,645,330   48,000,128 no null  int32
  22,645,330   24,000,128 no null  int16
  11,502,996  108,000,128 has null Int64
  11,382,772   60,000,128 has null Int32
  11,382,772   36,000,128 has null Int16
'''
import logging
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd


logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


SCRIPT_DIR = Path(__file__).parent.resolve()


def get_parquet_size(srs, use_dictionary=True):
    ''' gets the size of pandas series in a parquet file

        writes the pandas series to a temporary parquet file then writes an
        empty series to another temporary file and computes the difference to
        measure the size of the data
    '''
    size_data = None
    size_no_data = None
    with tempfile.TemporaryFile() as temp:
        df = srs.to_frame(name='col')
        df.to_parquet(temp, compression=None, use_dictionary=use_dictionary)
        size_data = temp.tell()

    with tempfile.TemporaryFile() as temp:
        df = srs.head(0).to_frame(name='col')
        df.to_parquet(temp, compression=None, use_dictionary=use_dictionary)
        size_no_data = temp.tell()

    return size_data - size_no_data


def print_memory_usage(srs, use_dictionary=True):
    def null_status(srs):
        if srs.isnull().sum() > 0:
            return 'has null'
        return 'no null'

    parquet_size = get_parquet_size(srs, use_dictionary)
    print('{:12,d} {:12,d} {:8s} {}'.format(
        parquet_size, srs.memory_usage(deep=True),
        null_status(srs), srs.dtypes))


def string_series_test():
    srs = pd.Series(['A', 'B'] * 6_000_000)
    print_memory_usage(srs)
    srs = pd.Series(['A', 'B'] * 6_000_000, dtype='string')
    print_memory_usage(srs)
    srs = pd.Series(['A', 'B', None] * 4_000_000)
    print_memory_usage(srs)
    srs = pd.Series(['A', 'B', None] * 4_000_000, dtype='string')
    print_memory_usage(srs)


def boolean_series_test():
    srs = pd.Series([False, True] * 6_000_000)
    print_memory_usage(srs)
    srs = pd.Series([False, True] * 6_000_000, dtype='boolean')
    print_memory_usage(srs)
    srs = pd.Series([False, True, None] * 4_000_000)
    print_memory_usage(srs)
    srs = pd.Series([False, True, None] * 4_000_000, dtype='boolean')
    print_memory_usage(srs)


def float_series_test():
    srs = pd.Series([1.0, 2.0] * 6_000_000)
    print_memory_usage(srs)
    srs = pd.Series([1.0, 2.0, None] * 4_000_000)
    print_memory_usage(srs)


def datetime_series_test():
    srs = pd.Series([np.datetime64('2020-01-01T03:30:30'),
                     np.datetime64('2020-01-02T03:30:30')] * 6_000_000)
    print_memory_usage(srs)  # 96,128 no null  datetime64[ns]
    srs = pd.Series([np.datetime64('2020-01-01T03:30:30'),
                     np.datetime64('2020-01-02T03:30:30'), None] * 4_000_000)
    print_memory_usage(srs)  # 96,128 no null  datetime64[ns]


def int_series_test():
    srs = pd.Series([1, 2] * 6_000_000, dtype='int64')
    print_memory_usage(srs)
    srs = pd.Series([1, 2] * 6_000_000, dtype='int32')
    print_memory_usage(srs)
    srs = pd.Series([1, 2] * 6_000_000, dtype='int16')
    print_memory_usage(srs)
    srs = pd.Series([1, 2] * 6_000_000, dtype='int8')
    print_memory_usage(srs)
    srs = pd.Series([1, 2, None] * 4_000_000)
    print_memory_usage(srs)
    srs = pd.Series([1, 2, None] * 4_000_000, dtype='Int64')
    print_memory_usage(srs)  # 108,128 has null Int64
    srs = pd.Series([1, 2, None] * 4_000_000, dtype='Int32')
    print_memory_usage(srs)  # 108,128 has null Int64
    srs = pd.Series([1, 2, None] * 4_000_000, dtype='Int16')
    print_memory_usage(srs)  # 108,128 has null Int64
    srs = pd.Series([1, 2, None] * 4_000_000, dtype='Int8')
    print_memory_usage(srs)  # 24,128 has null Int8


def int_random_series_test():
    int_data = np.random.randint(
        0, 30_000, (6_000_000,), dtype='int64').tolist()

    print('no dictionary encoding', '=' * 20)
    srs = pd.Series(int_data * 2, dtype='int64')
    print_memory_usage(srs, use_dictionary=False)
    srs = pd.Series(int_data * 2, dtype='int32')
    print_memory_usage(srs, use_dictionary=False)
    srs = pd.Series(int_data * 2, dtype='int16')
    print_memory_usage(srs, use_dictionary=False)

    srs = pd.Series(int_data + [None] * 6_000_000, dtype='Int64')
    print_memory_usage(srs, use_dictionary=False)
    srs = pd.Series(int_data + [None] * 6_000_000, dtype='Int32')
    print_memory_usage(srs, use_dictionary=False)
    srs = pd.Series(int_data + [None] * 6_000_000, dtype='Int16')
    print_memory_usage(srs, use_dictionary=False)

    print('with dictionary encoding', '=' * 20)
    srs = pd.Series(int_data * 2, dtype='int64')
    print_memory_usage(srs, use_dictionary=True)
    srs = pd.Series(int_data * 2, dtype='int32')
    print_memory_usage(srs, use_dictionary=True)
    srs = pd.Series(int_data * 2, dtype='int16')
    print_memory_usage(srs, use_dictionary=True)

    srs = pd.Series(int_data + [None] * 6_000_000, dtype='Int64')
    print_memory_usage(srs, use_dictionary=True)
    srs = pd.Series(int_data + [None] * 6_000_000, dtype='Int32')
    print_memory_usage(srs, use_dictionary=True)
    srs = pd.Series(int_data + [None] * 6_000_000, dtype='Int16')
    print_memory_usage(srs, use_dictionary=True)


def main():
    print('{:12s} {:12s} {:8s} {:s}'.format(
        'parquet size', 'pandas size', 'null', 'dtype'))
    print('string test', '=' * 30)
    string_series_test()

    print('boolean test', '=' * 30)
    boolean_series_test()

    print('float test', '=' * 30)
    float_series_test()

    print('datetime test', '=' * 30)
    datetime_series_test()

    print('int test', '=' * 30)
    int_series_test()

    print('random int test', '=' * 30)
    int_random_series_test()


if __name__ == '__main__':
    main()
