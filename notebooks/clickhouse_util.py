import inspect
import difflib

import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker
from sqlalchemy import func

import numpy
import pandas

from typing import List


def get_null_count(conn_str, table):
    engine = sa.create_engine('clickhouse://default@10.0.0.2:8123/default')

    metadata = sa.MetaData(bind=engine)
    metadata.reflect(only=[table])
    tbl = metadata.tables[table]

    Session = sessionmaker(bind=engine)
    session = Session()

    col_names = [col.name for col in tbl.columns]
    null_sums = [
        func.sum(tbl.c[name].is_(None)) for name in col_names]

    qry = session.query(*null_sums)
    return dict(zip(col_names, qry.first()))


numpy_types = (
    'bool',  # bool - Boolean (True or False) stored as a byte
    'byte',  # signed char - Platform-defined
    'ubyte',  # unsigned char - Platform-defined
    'short',  # short - Platform-defined
    'ushort',  # unsigned short - Platform-defined
    'intc',  # int - Platform-defined
    'uintc',  # unsigned int - Platform-defined
    'int_',  # long - Platform-defined
    'uint',  # unsigned long - Platform-defined
    'longlong',  # long long - Platform-defined
    'ulonglong',  # unsigned long long - Platform-defined
    'half',  # Half - precision float: sign bit, 5 bits exponent,
             # 10 bits mantissa
    'float16',  # Half - precision float: sign bit, 5 bits exponent,
             # 10 bits mantissa
    'single',  # float - Platform-defined single precision float: typically
             # sign bit, 8 bits exponent, 23 bits mantissa
    'double',  # double - Platform-defined double precision float: typically
             # sign bit, 11 bits exponent, 52 bits mantissa.
    'longdouble',  # long double - Platform-defined extended-precision float
    'csingle',  # float complex - Complex number, represented by two
                # single-precision floats (real and imaginary components)
    'cdouble',  # double complex - Complex number, represented by two
                # double-precision floats (real and imaginary components).
    'clongdouble',  # long double complex - Complex number, represented by
                    # two extended-precision floats (real and imaginary
                    # components).
    'int8',  # int8_t  - Byte (-128 to 127)
    'int16',  # int16_t  - Integer (-32768 to 32767)
    'int32',  # int32_t  - Integer (-2147483648 to 2147483647)
    'int64',  # int64_t  - Integer (-9223372036854775808 to
              # 9223372036854775807)
    'uint8',  # uint8_t  - Unsigned integer (0 to 255)
    'uint16',  # uint16_t  - Unsigned integer (0 to 65535)
    'uint32',  # uint32_t  - Unsigned integer (0 to 4294967295)
    'uint64',  # uint64_t  - Unsigned integer (0 to 18446744073709551615)
    'intp',  # intptr_t  - Integer used for indexing, typically the same as
             # ssize_t
    'uintp',  # uintptr_t  - Integer large enough to hold a pointer
    'float32',  # float
    'float64',  # double  - Note that this matches the precision of the
                # builtin python float.
    'float_',  # double  - Note that this matches the precision of the builtin
               # python float.
    'complex64',  # float complex  - Complex number, represented by two 32-bit
                  # floats (real and imaginary components)
    'complex128',  # double complex  - Note that this matches the precision of
                   # the builtin python complex.
    'complex_',  # double complex  - Note that this matches the precision of
                 # the builtin python complex.
    'object_'
)


def get_numpy_types():
    types = {}
    for name, data in inspect.getmembers(numpy, inspect.isclass):
        if name in numpy_types:
            types[name] = data
    return types


pandas_types = (
    'BooleanDtype',
    'CategoricalDtype',
    'DatetimeTZDtype',
    'Int16Dtype',
    'Int32Dtype',
    'Int64Dtype',
    'Int8Dtype',
    'IntervalDtype',
    'PeriodDtype',
    'SparseDtype',
    'StringDtype',
    'UInt16Dtype',
    'UInt32Dtype',
    'UInt64Dtype',
    'UInt8Dtype'
)


def get_pandas_types():
    types = {}
    for name, data in inspect.getmembers(pandas, inspect.isclass):
        if name in pandas_types:
            types[name] = data
    return types


clickhouse_types = (
    'Boolean',
    'Date',
    'DateTime',
    'Float32',
    'Float64',
    'UInt8',
    'UInt16',
    'UInt32',
    'UInt64',
    'Int8',
    'Int16',
    'Int32',
    'Int64',
    'String'
)


def get_clickhouse_types():
    return clickhouse_types


clickhouse_type_range = {
    'Int8': [-128, 127],
    'Int16': [-32768, 32767],
    'Int32': [-2147483648, 2147483647],
    'Int64': [-9223372036854775808, 9223372036854775807],
    'UInt8': [0, 255],
    'UInt16': [0, 65535],
    'UInt32': [0, 4294967295],
    'UInt64': [0, 18446744073709551615],
}


def get_clickhouse_type_range(clickhouse_type):
    return clickhouse_type_range(clickhouse_type)


numpy_clickhouse_map = {
    numpy.float64: 'Float64',
    numpy.int64: 'Int64',
    numpy.object_: 'String'
}


def get_clickhouse_type(numpy_type):
    return numpy_clickhouse_map[numpy_type]


def get_clickhouse_type_efficient(numpy_type, srs):
    ''' Use a pandas series to get an efficient type

        example: Int16 instead of Int64
        TODO: not yet implemented
    '''
    return numpy_clickhouse_map[numpy_type]


def check_sorting_key(sorting_key, columns):
    if sorting_key not in columns:
        closest_matches = difflib.get_close_matches(sorting_key, columns)
        hint = ''
        if len(closest_matches) > 0:
            hint = '\nDid you mean "{}"?'.format(closest_matches[0])
        raise ValueError('Sorting key "{}" not a dataframe column.{}'.format(
            sorting_key, hint))


def get_nullable_type(field_type, nullable):
    assert field_type in clickhouse_types
    if nullable:
        return 'Nullable({})'.format(field_type)
    return field_type


def get_clickhouse_create_sql(df, table_name, sorting_keys: List[str]) -> str:
    ''' create sql statement from dataframe

        sorting_keys must be non-nullable
    '''
    for sorting_key in sorting_keys:
        check_sorting_key(sorting_key, df.columns)

    create_prefix = 'create table {} (\n'.format(table_name)
    field_lines = []
    for col_name, numpy_type in df.dtypes.items():
        field_type = get_clickhouse_type(numpy_type.type)
        nullable = False if col_name in sorting_key else True
        nullable_type = get_nullable_type(field_type, nullable)
        field_line = '\t{} {}'.format(
            col_name, nullable_type)
        field_lines.append(field_line)
    create_suffix = ')\nEngine = MergeTree\nOrder by {}'.format(sorting_key)
    return create_prefix + ',\n'.join(field_lines) + '\n' + create_suffix
