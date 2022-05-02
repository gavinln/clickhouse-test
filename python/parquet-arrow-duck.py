"""
Use Parquet files with arrow and duckdb
export PYTHONBREAKPOINT=IPython.embed
"""
import logging
import time
import pathlib
import sys
import numpy as np

import pyarrow as pa
import pyarrow.dataset as ds
from pyarrow import fs
import pyarrow.parquet as pq
import pyarrow.compute as pc

import duckdb
import pandas as pd


log = logging.getLogger(__name__)
SCRIPT_DIR = pathlib.Path(__file__).parent.resolve()


def main2():
    pq_file = SCRIPT_DIR / '1988_cleaned.parq'
    parquet_file = pq.ParquetFile(pq_file)
    breakpoint()


def create_dataframe(rows):
    data = pd.DataFrame(np.random.rand(rows, 2), columns=['value1', 'value2'])
    data['name'] = np.random.choice(['apple', 'banana', 'orange'], size=rows)
    return data


def write_dataframe_to_parquet(df, parquet_file):
    pq_file = pathlib.Path(parquet_file)
    if not pq_file.exists():
        table = pa.Table.from_pandas(df)
        pq.write_table(table, parquet_file, compression='gzip')


def read_table_from_parquet(parquet_file):
    pq_file = pathlib.Path(parquet_file)
    if not pq_file.exists():
        sys.exit(f'File {parquet_file} does not exist.')
    local = fs.LocalFileSystem()
    table = pq.read_table(pq_file, filesystem=local)
    return table


def main():
    rows = 40_000_000
    # rows = 4
    df = create_dataframe(rows)
    parquet_file = 'temp.parquet'
    write_dataframe_to_parquet(df, parquet_file)

    start = time.time()
    table = read_table_from_parquet(parquet_file)
    name_counts = pc.value_counts(table.column("name"))
    name_counts_df = pd.DataFrame.from_records(name_counts.to_pylist())
    elapsed = time.time() - start
    print(f"Arrow Elapsed seconds {elapsed:.4f}")
    print(name_counts_df)

    con = duckdb.connect(database=':memory:', read_only=False)
    sql = """
        select name, count(*)
        from parquet_scan('{}')
        group by name
    """
    sql_query = sql.format(f"{parquet_file}")
    start = time.time()
    df = con.execute(sql_query).fetchdf()
    elapsed = time.time() - start
    print(f"DuckDB Elapsed seconds {elapsed:.4f}")
    print(df)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
