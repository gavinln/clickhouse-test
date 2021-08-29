'''
Use metaflow to read and write to S3
'''
import logging
import time

import pathlib

import pyarrow.parquet as pq

import typer
import duckdb


logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)
SCRIPT_DIR = pathlib.Path(__file__).parent.resolve()
app = typer.Typer(help='command line tools for parquet files')


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


@app.command()
def column_info(parquet_file: str):
    ' get column information '
    check_file_exists(parquet_file)
    parquet_file = pq.ParquetFile(parquet_file)
    schema = parquet_file.metadata.schema
    for idx, col_name in enumerate(schema.names):
        print('{}/{} {}'.format(
            idx + 1, len(schema.names), str(schema.column(idx))))


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

    head_row_groups = 10
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

    head_row_groups = 10
    total_row_groups = get_min_row_groups(
        metadata.num_row_groups, head_row_groups, all)

    col_idx = metadata.schema.names.index(column_name)

    for row_idx in range(total_row_groups):
        row_col_meta = metadata.row_group(row_idx).column(col_idx)
        if row_col_meta.is_stats_set:
            print(row_col_meta.statistics)

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

    sql = "select Year, count(*) from parquet_scan('{}') group by Year"
    sql_query = sql.format(f'{parquet_file}')

    start = time.time()
    df = con.execute(sql_query).fetchdf()
    elapsed = time.time() - start
    print(f'Elapsed {elapsed:.4f}')
    print(df)


def main():
    app()


if __name__ == '__main__':
    main()
