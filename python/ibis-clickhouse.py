import sys
from pathlib import Path

import pandas as pd
import sqlalchemy as sa

import ibis
import fire

import pyarrow.parquet as pq

from IPython import embed


def clickhouse_sqlalchemy(engine):
    ''' Use pandas and sqlalchemy to read data
    '''
    sql = 'select * from flight limit 100000'
    df = pd.read_sql(sql, engine)
    print(df.columns)

    metadata = sa.MetaData(bind=engine)
    metadata.reflect(only=['flight'])
    flight_tbl = metadata.tables['flight']

    for column in flight_tbl.columns:
        print(column.name)
        # print(column.type)  # does not work for nullable types in Clickhouse


def ibis_interface(clickhouse_host):
    conn = ibis.clickhouse.connect(
        host=clickhouse_host, port=9000, database='default')

    table = conn.table('flight')
    ibis.options.interactive = True
    print(table.Origin.value_counts())


def str_to_int(int_str):
    try:
        return int(int_str)
    except ValueError:
        pass
    return None


def _check_int_gte_zero(int_str):
    val = str_to_int(int_str)
    if val is None:
        sys.exit('Invalid value {}. Should be a number'.format(int_str))

    if val < 0:
        sys.exit('Value {} should be greater than or equal to zero'.format(
            int_str))
    return val


def _check_file(parquet_file):
    pq_file = Path(parquet_file)
    if not pq_file.exists():
        sys.exit('File {} does not exist'.format(
            parquet_file))

    with pq_file.open('rb') as f:
        data = f.read(4)
        if data != b'PAR1':
            sys.exit('File {} is not a valid parquet file'.format(
                parquet_file))
    return None


def _parquet_rowgroup(parquet_file, rowgroup_idx):
    assert isinstance(rowgroup_idx, int), 'rowgroup_idx not valid'
    parq_file = pq.ParquetFile(parquet_file)
    metadata = parq_file.metadata

    if rowgroup_idx >= metadata.num_row_groups:
        sys.exit(
            'rowgroup_idx should be less than {}'.format(
                metadata.num_row_groups))

    return parq_file.metadata.row_group(rowgroup_idx)


def _parquet_rowgroup_column(parquet_file, rowgroup_idx, col_idx):
    assert isinstance(rowgroup_idx, int), 'rowgroup_idx is not valid'
    assert isinstance(col_idx, int), 'col_idx is not valid'
    rowgroup = _parquet_rowgroup(parquet_file, rowgroup_idx)
    if col_idx >= rowgroup.num_columns:
        sys.exit('col_idx should be less than {}'.format(
            rowgroup.num_columns))
    rowgroup_column = rowgroup.column(col_idx)
    return rowgroup_column


def _parquet_column_index(parquet_file, column_name):
    parq_file = pq.ParquetFile(parquet_file)
    metadata = parq_file.metadata
    schema = metadata.schema
    if column_name not in schema.names:
        sys.exit('Invalid column name {}'.format(
            column_name))
    idx = schema.names.index(column_name)
    return idx


def min_none(val1, val2):
    if val1 is None:
        return val2
    if val2 is None:
        return val1
    return min(val1, val2)


def max_none(val1, val2):
    if val1 is None:
        return val2
    if val2 is None:
        return val1
    return max(val1, val2)


class Parquet:

    def metadata(self, parquet_file):
        _check_file(parquet_file)
        parq_file = pq.ParquetFile(parquet_file)
        metadata = parq_file.metadata
        print(metadata)

    def schema(self, parquet_file):
        parq_file = pq.ParquetFile(parquet_file)
        metadata = parq_file.metadata
        print(metadata.schema)

    def column(self, parquet_file, column_name):
        col_idx = _parquet_column_index(parquet_file, column_name)
        assert col_idx >= 0, 'Invalid column {}'.format(column_name)
        parq_file = pq.ParquetFile(parquet_file)
        metadata = parq_file.metadata
        print(metadata.schema.column(col_idx))

    def rowgroup(self, parquet_file, rowgroup_idx):
        ' display a Parquet row group (0 indexed) '
        idx = _check_int_gte_zero(rowgroup_idx)
        rowgroup = _parquet_rowgroup(parquet_file, idx)
        print(rowgroup)

    def rowgroup_column(self, parquet_file, rowgroup_idx, col_idx):
        r_idx = _check_int_gte_zero(rowgroup_idx)
        c_idx = _check_int_gte_zero(col_idx)
        rowgroup_column = _parquet_rowgroup_column(
            parquet_file, r_idx, c_idx)
        print(rowgroup_column)

    def column_min_max(self, parquet_file, column_name):
        col_idx = _parquet_column_index(parquet_file, column_name)
        assert col_idx >= 0, 'Invalid column {}'.format(column_name)
        parq_file = pq.ParquetFile(parquet_file)
        metadata = parq_file.metadata
        global_min = None
        global_max = None

        for idx in range(metadata.num_row_groups):
            rowgroup = metadata.row_group(idx)
            stats = rowgroup.column(col_idx).statistics
            if stats.has_min_max:
                global_min = min_none(global_min, stats.min)
                global_max = max_none(global_max, stats.max)
            else:
                sys.exit(
                    'Missing column statistics for {}'.format(column_name))
        print(global_min, global_max)


def main():
    # Create SqlAlchemy engine
    clickhouse_host = '127.0.0.1'
    clickhouse_host = '10.0.0.2'
    ch_url = 'clickhouse://default@{}:8123/default'.format(
        clickhouse_host)
    engine = sa.create_engine(ch_url)

    fire.Fire({
        'sqlalchemy': lambda: clickhouse_sqlalchemy(engine),
        'ibis': lambda: ibis_interface(clickhouse_host),
        'parquet': Parquet
    })


if __name__ == '__main__':
    main()

