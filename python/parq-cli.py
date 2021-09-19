'''
Use metaflow to read and write to S3
'''
import logging
import time

import pathlib
import sys

from distutils.spawn import find_executable

from typing import NamedTuple

from subprocess import check_output
import pyarrow.parquet as pq

import typer
import duckdb
import pandas as pd


logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)
SCRIPT_DIR = pathlib.Path(__file__).parent.resolve()
app = typer.Typer(help='command line tools for parquet files')


def to_string_ljustify(df):
    ''' pandas dataframe to a string with left justified text
    '''
    col_formatters = []
    for col_name in df.columns:
        col = df[col_name]
        if col.dtype == 'object':
            col_len_max = col.apply(len).max()
            col_format = '{{:<{}s}}'.format(col_len_max)
            col_formatters.append(col_format.format)
        else:
            col_formatters.append(None)

    # left justify strings
    str_df = df.to_string(index=False, formatters=col_formatters)
    # remove trailing whitespaces
    return '\n'.join(line.rstrip() for line in str_df.split('\n'))

    # default printing is right justified
    # return df.to_string(index=False)


def print_tty_redir(df):
    ''' print data frame to a tty (partial) or redirected output (full)
    '''
    if df is not None:
        if sys.stdout.isatty():
            print(df.to_string(index=False))
        else:
            with pd.option_context("display.max_rows", None,
                                   "display.max_columns", None):
                print(to_string_ljustify(df))


def check_file_exists(file_name: str):
    data_file = pathlib.Path(file_name)
    if not data_file.exists():
        typer.echo(f'File {data_file} does not exist')
        raise typer.Exit()


@app.command()
def metadata(parquet_file: str):
    ' get metadata '
    check_file_exists(parquet_file)
    parquet_file = pq.ParquetFile(parquet_file)
    metadata = parquet_file.metadata
    print(metadata)


@app.command()
def schema(parquet_file: str):
    ' get column schema '
    check_file_exists(parquet_file)
    parquet_file = pq.ParquetFile(parquet_file)
    metadata = parquet_file.metadata
    print(metadata.schema)


@app.command()
def column_names(parquet_file: str):
    ' get column names '
    check_file_exists(parquet_file)
    parquet_file = pq.ParquetFile(parquet_file)
    metadata = parquet_file.metadata
    print('\n'.join(metadata.schema.names))


def column_schema_to_dict(column_schema) -> dict:
    attrs = ['name', 'path', 'max_definition_level', 'max_repetition_level',
             'physical_type']
    return  {attr:getattr(column_schema, attr) for attr in attrs }


@app.command()
def column_info(parquet_file: str):
    ' get column information '
    check_file_exists(parquet_file)
    parquet_file = pq.ParquetFile(parquet_file)
    schema = parquet_file.metadata.schema

    column_schema_list = []
    for idx, col_name in enumerate(schema.names):
        # print('{}/{} {}'.format(
        #     idx + 1, len(schema.names), str(schema.column(idx))))
        column_schema_list.append(column_schema_to_dict(schema.column(idx)))

    df = pd.DataFrame.from_records(column_schema_list)
    print_tty_redir(df)


def check_column_exists(parquet_file: str, column_name: str):
    ''' checks whether a column exists in a Parquet file

        returns parquet metadata
    '''
    parquet_file = pq.ParquetFile(parquet_file)
    schema = parquet_file.metadata.schema
    if column_name not in schema.names:
        typer.echo(f'Invalid column {column_name}')
        raise typer.Exit()
    return parquet_file.metadata


def get_min_row_groups(
        all_row_groups: int, head_row_groups: int, all: bool=False):
    ' returns smaller number of row groups unless all is True '
    if all:
        return all_row_groups
    return min(all_row_groups, head_row_groups)


@app.command()
def column_stats_set(parquet_file: str, all: bool=False):
    ' get number of row groups with column stats '
    check_file_exists(parquet_file)
    parquet_file = pq.ParquetFile(parquet_file)
    metadata = parquet_file.metadata

    head_row_groups = 5
    total_row_groups = get_min_row_groups(
        metadata.num_row_groups, head_row_groups, all)

    column_stats = [0] * metadata.num_columns
    for row_idx in range(total_row_groups):
        for col_idx in range(metadata.num_columns):
            if metadata.row_group(row_idx).column(col_idx).is_stats_set:
                column_stats[col_idx] += 1

    for count, column in zip(column_stats, metadata.schema.names):
        print('{:10d}/{}\t{}'.format(count, total_row_groups, column))


@app.command()
def column_stats(parquet_file: str, column_name: str, all: bool=False):
    ' get column stats for a single column '
    check_file_exists(parquet_file)
    metadata = check_column_exists(parquet_file, column_name)

    head_row_groups = 5
    total_row_groups = get_min_row_groups(
        metadata.num_row_groups, head_row_groups, all)

    col_idx = metadata.schema.names.index(column_name)

    stat_list = []
    for row_idx in range(total_row_groups):
        row_col_meta = metadata.row_group(row_idx).column(col_idx)
        if row_col_meta.is_stats_set:
            stat_list.append(row_col_meta.statistics.to_dict())

    df = pd.DataFrame.from_records(stat_list)
    print(df)


'''
# duckdb parquet queries
describe select * from parquet_scan('{})
select * from parquet_metadata('{}')
select * from parquet_schema('{}')
'''

@app.command()
def duck(parquet_file: str):
    ' query parquet file using duckdb '
    check_file_exists(parquet_file)
    con = duckdb.connect(database=':memory:', read_only=False)

    sql = """
        select Year, count(*) ct, count(distinct Carrier) carrier_uniq_ct
        from parquet_scan('{}')
        group by Year
    """
    sql_query = sql.format(f'{parquet_file}')

    start = time.time()
    df = con.execute(sql_query).fetchdf()
    elapsed = time.time() - start
    print(f'Elapsed {elapsed:.4f}')
    print(df)


ParquetClickhouseType = NamedTuple(
    "ParquetClickhouseType", [('parquet', str), ('clickhouse', str)])

pc_types = [pc_type.split() for pc_type in (
    'UINT8 UInt8',
    'INT8 Int8',
    'UINT16 UInt16',
    'INT16 Int16',
    'UINT32 UInt32',
    'INT32 Int32',
    'UINT64 UInt64',
    'INT64 Int64',
    'FLOAT Float32',
    'DOUBLE Float64',
    'BYTE_ARRAY String')]
pc_dict = { p_type: c_type for p_type, c_type in pc_types }
cp_dict = { c_type: p_type for p_type, c_type in pc_types }


def check_executable(executable_name):
    ' return False if executable is not available '
    prg = find_executable(executable_name)
    if prg is None:
        sys.exit(f'Cannot find {executable_name}. Is it in the PATH?')
    output = check_output('{} --version'.format(prg), shell=True)
    print('Found {}'.format(output.decode('unicode_escape')))


def clickhouse_types(parquet_file):
    ' get parquet columns as clickhouse types string '
    pq_file = pq.ParquetFile(parquet_file)
    schema = pq_file.metadata.schema
    pq_types = []
    for idx, col_name in enumerate(schema.names):
        col_name = schema.column(idx).name
        col_type = schema.column(idx).physical_type
        pq_types.append([col_name, pc_dict[col_type]])
    return ', '.join(
        f'{col_name} {col_type}' for col_name, col_type in pq_types)


@app.command()
def clickhouse(parquet_file: str):
    ' query parquet file using clickhouse-local '
    check_file_exists(parquet_file)

    executable_name = 'clickhouse-local'
    check_executable(executable_name)
    ch_types_str = clickhouse_types(parquet_file)
    print(ch_types_str)

    sql = """
        select Year, count(*) ct, count(distinct Carrier) carrier_uniq_ct
        from file(
            '{}', Parquet,
            '{}'
        )
        group by Year
    """.format(parquet_file, ch_types_str)

    clickhouse_query = sql.replace("\n", " ")

    start = time.time()
    output = check_output(
        [executable_name, '--query', clickhouse_query], shell=False)
    elapsed = time.time() - start
    print(f'Elapsed {elapsed:.4f}')
    print(output.decode('utf-8').strip())


def main():
    app()


if __name__ == '__main__':
    main()
