"""
Use metaflow to read and write to S3
"""
import logging
import time
import os
import pathlib
import sys

from distutils.spawn import find_executable
from clickhouse_driver import Client

from typing import NamedTuple

from subprocess import check_output

import pyarrow as pa
from pyarrow import fs
import pyarrow.parquet as pq
import pyarrow.compute as pc

import duckdb
import pandas as pd

import datafusion
from datafusion import functions as f
from datafusion import col
from datafusion import literal

import fire


log = logging.getLogger(__name__)
SCRIPT_DIR = pathlib.Path(__file__).parent.resolve()


def to_string_ljustify(df):
    """pandas dataframe to a string with left justified text"""
    col_formatters = []
    for col_name in df.columns:
        col = df[col_name]
        if col.dtype == "object":
            col_len_max = col.apply(len).max()
            col_format = "{{:<{}s}}".format(col_len_max)
            col_formatters.append(col_format.format)
        else:
            col_formatters.append(None)

    # left justify strings
    str_df = df.to_string(index=False, formatters=col_formatters)
    # remove trailing whitespaces
    return "\n".join(line.rstrip() for line in str_df.split("\n"))

    # default printing is right justified
    # return df.to_string(index=False)


def print_tty_redir(df):
    """print data frame to a tty (partial) or redirected output (full)"""
    if df is not None:
        if sys.stdout.isatty():
            print(df.to_string(index=False))
        else:
            with pd.option_context(
                "display.max_rows", None, "display.max_columns", None
            ):
                print(to_string_ljustify(df))


def check_file_exists(file_name: str):
    data_file = pathlib.Path(file_name)
    if not data_file.exists():
        sys.exit(f"File {data_file} does not exist")


def metadata(parquet_file: str):
    "get metadata"
    check_file_exists(parquet_file)
    parquet_file = pq.ParquetFile(parquet_file)
    metadata = parquet_file.metadata
    print(metadata)


def schema(parquet_file: str):
    "get column schema"
    check_file_exists(parquet_file)
    parquet_file = pq.ParquetFile(parquet_file)
    metadata = parquet_file.metadata
    print(metadata.schema)


def column_names(parquet_file: str):
    "get column names"
    check_file_exists(parquet_file)
    parquet_file = pq.ParquetFile(parquet_file)
    metadata = parquet_file.metadata
    print("\n".join(metadata.schema.names))


def column_schema_to_dict(column_schema) -> dict:
    attrs = [
        "name",
        "path",
        "max_definition_level",
        "max_repetition_level",
        "physical_type",
    ]
    return {attr: getattr(column_schema, attr) for attr in attrs}


def column_info(parquet_file: str):
    "get column information"
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
    """checks whether a column exists in a Parquet file

    returns parquet metadata
    """
    parquet_file = pq.ParquetFile(parquet_file)
    schema = parquet_file.metadata.schema
    if column_name not in schema.names:
        sys.exit(f"Invalid column {column_name}")
    return parquet_file.metadata


def get_min_row_groups(
    all_row_groups: int, head_row_groups: int, all: bool = False
):
    "returns smaller number of row groups unless all is True"
    if all:
        return all_row_groups
    return min(all_row_groups, head_row_groups)


def column_stats_set(parquet_file: str, all: bool = False):
    "get number of row groups with column stats"
    check_file_exists(parquet_file)
    parquet_file = pq.ParquetFile(parquet_file)
    metadata = parquet_file.metadata

    head_row_groups = 5
    total_row_groups = get_min_row_groups(
        metadata.num_row_groups, head_row_groups, all
    )

    column_stats = [0] * metadata.num_columns
    for row_idx in range(total_row_groups):
        for col_idx in range(metadata.num_columns):
            if metadata.row_group(row_idx).column(col_idx).is_stats_set:
                column_stats[col_idx] += 1

    for count, column in zip(column_stats, metadata.schema.names):
        print("{:10d}/{}\t{}".format(count, total_row_groups, column))


def column_stats(parquet_file: str, column_name: str, all: bool = False):
    "get column stats for a single column"
    check_file_exists(parquet_file)
    metadata = check_column_exists(parquet_file, column_name)

    head_row_groups = 5
    total_row_groups = get_min_row_groups(
        metadata.num_row_groups, head_row_groups, all
    )

    col_idx = metadata.schema.names.index(column_name)

    stat_list = []
    for row_idx in range(total_row_groups):
        row_col_meta = metadata.row_group(row_idx).column(col_idx)
        if row_col_meta.is_stats_set:
            stat_list.append(row_col_meta.statistics.to_dict())

    df = pd.DataFrame.from_records(stat_list)
    print(df)


"""
# duckdb parquet queries
describe select * from parquet_scan('{})
select * from parquet_metadata('{}')
select * from parquet_schema('{}')
"""


def duck(parquet_file: str):
    "query parquet file using duckdb"
    check_file_exists(parquet_file)
    con = duckdb.connect(database=":memory:", read_only=False)

    sql = """
        select Year, count(*) ct, count(distinct Carrier) carrier_uniq_ct
        from parquet_scan('{}')
        group by Year
    """
    sql_query = sql.format(f"{parquet_file}")

    start = time.time()
    df = con.execute(sql_query).fetchdf()
    elapsed = time.time() - start
    print(f"Elapsed {elapsed:.4f}")
    print(df)


ParquetClickhouseType = NamedTuple(
    "ParquetClickhouseType", [("parquet", str), ("clickhouse", str)]
)

pc_types = [
    pc_type.split()
    for pc_type in (
        "UINT8 UInt8",
        "INT8 Int8",
        "UINT16 UInt16",
        "INT16 Int16",
        "UINT32 UInt32",
        "INT32 Int32",
        "UINT64 UInt64",
        "INT64 Int64",
        "FLOAT Float32",
        "DOUBLE Float64",
        "BYTE_ARRAY String",
    )
]
pc_dict = {p_type: c_type for p_type, c_type in pc_types}
cp_dict = {c_type: p_type for p_type, c_type in pc_types}


def check_executable(executable_name):
    "return False if executable is not available"
    prg = find_executable(executable_name)
    if prg is None:
        sys.exit(f"Cannot find {executable_name}. Is it in the PATH?")
    output = check_output("{} --version".format(prg), shell=True)
    print("Found {}".format(output.decode("unicode_escape")))


def get_clickhouse_types(parquet_file):
    "get parquet columns as clickhouse types string"
    pq_file = pq.ParquetFile(parquet_file)
    schema = pq_file.metadata.schema
    pq_types = []
    for idx, col_name in enumerate(schema.names):
        col_name = schema.column(idx).name
        col_type = schema.column(idx).physical_type
        pq_types.append([col_name, pc_dict[col_type]])
    return ", ".join(
        f"{col_name} {col_type}" for col_name, col_type in pq_types
    )


def ch_local(parquet_file: str):
    "query parquet file using clickhouse-local"
    check_file_exists(parquet_file)

    executable_name = "clickhouse-local"
    check_executable(executable_name)
    ch_types_str = get_clickhouse_types(parquet_file)
    print(ch_types_str)

    sql = """
        select Year, count(*) ct, count(distinct Carrier) carrier_uniq_ct
        from file(
            '{}', Parquet,
            '{}'
        )
        group by Year
    """.format(
        parquet_file, ch_types_str
    )

    clickhouse_query = sql.replace("\n", " ")

    start = time.time()
    output = check_output(
        [executable_name, "--query", clickhouse_query], shell=False
    )
    elapsed = time.time() - start
    print(f"Elapsed {elapsed:.4f}")
    print(output.decode("utf-8").strip())


def ch_server_184m():
    "query clickhouse-server online data"

    ch_password = os.environ.get("CH_PASSWORD")
    ch_user = "default"
    if ch_password is None:
        msg = "Clickhouse password for user {} not specified. Set CH_PASSWORD"
        sys.exit(msg.format(ch_user))
    client = Client("127.0.0.1", user=ch_user, password=ch_password)
    # for database in client.execute('show databases'):
    #     print(database)
    sql = """
        select Year, count(*) ct, count(distinct Carrier) carrier_uniq_ct
        from datasets.ontime
        group by Year
    """
    start = time.time()
    results = client.execute(sql)
    elapsed = time.time() - start
    print(f"Elapsed {elapsed:.4f}")
    for row in results:
        print(*row)


def arrow_parquet_example(parquet_file: str):
    "use arrow to read parquet files"

    check_file_exists(parquet_file)
    local = fs.LocalFileSystem()

    print("Counts by Year:")
    start = time.time()
    tbl = pq.read_table(parquet_file, filesystem=local)
    result = pc.value_counts(tbl.column("Year"))
    elapsed = time.time() - start
    print(f"Elapsed {elapsed:.4f}")
    print(result.to_pandas())


def datafusion_parquet(parquet_file: str):
    "use datafusion to process parquet files"

    check_file_exists(parquet_file)
    print("Parquet file must have extension .parquet")
    ctx = datafusion.ExecutionContext()
    ctx.register_parquet("t", parquet_file)
    sql = """
        select Year, count(*) ct, count(distinct Carrier) carrier_uniq_ct
        from t
        group by Year
    """
    start = time.time()
    sql_result = ctx.sql(sql).collect()
    result = pa.Table.from_batches(sql_result)
    elapsed = time.time() - start
    print(f"Elapsed {elapsed:.4f}")
    print(result.to_pandas())

    df = ctx.table("t")
    start = time.time()
    batches = df.aggregate(
        [col("Year")],
        [
            f.count(col("Year")).alias("Year_ct"),
            f.approx_distinct(col("Carrier")).alias("approx_dist"),
        ],
    )
    result = pa.Table.from_batches(batches.collect())
    elapsed = time.time() - start
    print(f"Elapsed {elapsed:.4f}")
    print(result.to_pandas())


def arrow_compute_example():
    "arrow compute examples"

    # https://arrow.apache.org/cookbook/py/data.html

    tbl = pa.table({"name": list("aabccc"), "value": list(range(6))})
    print("table data")
    print(tbl.to_pandas())

    print("count name column:", pc.count(tbl.column("name")).as_py())

    print(
        "count name distinct column:",
        pc.count_distinct(tbl.column("name")).as_py(),
    )

    print("min name column:", pc.min(tbl.column("name")).as_py())

    print("min value column:", pc.min(tbl.column("value")).as_py())

    print("max value column:", pc.max(tbl.column("value")).as_py())

    print("min, max value column:", pc.min_max(tbl.column("value")).as_py())

    print("mean value column:", pc.mean(tbl.column("value")).as_py())

    print(
        "name value counts:", pc.value_counts(tbl.column("name")).to_pylist()
    )

    print(
        "multiply value by 2:", pc.multiply(tbl.column("value"), 2).to_pylist()
    )

    print(
        "multiply value by itself:",
        pc.multiply(tbl.column("value"), tbl.column("value")).to_pylist(),
    )

    print(
        "rows where value is greater than 2",
        pc.filter(tbl, pc.greater(tbl.column("value"), 2)).to_pandas(),
    )


def datafusion_compute_example():
    "datafusion compute examples"

    # https://arrow.apache.org/cookbook/py/data.html

    tbl = pa.table({"name": list("aabccc"), "value": list(range(6))})

    ctx = datafusion.ExecutionContext()

    df = ctx.create_dataframe([tbl.to_batches()])

    print("table data")
    print(tbl.to_pandas())

    # ctx.register_record_batches('t', [tbl.to_batches()])
    # batches = ctx.sql('select name, sum(value) from t group by name')
    # result = pa.Table.from_batches(batches.collect())
    # result.to_pandas()

    df = ctx.create_dataframe([tbl.to_batches()])

    batches = df.aggregate([], [f.count(col("name")).alias("name")]).collect()
    print("count name column:", pa.Table.from_batches(batches).to_pydict())

    batches = df.aggregate(
        [], [f.approx_distinct(col("name")).alias("name")]
    ).collect()
    print(
        "count name distinct column:",
        pa.Table.from_batches(batches).to_pydict(),
    )

    batches = df.aggregate([], [f.min(col("name")).alias("name")]).collect()
    print("min name column:", pa.Table.from_batches(batches).to_pydict())

    batches = df.aggregate([], [f.min(col("value")).alias("value")]).collect()
    print("min value column:", pa.Table.from_batches(batches).to_pydict())

    batches = df.aggregate([], [f.max(col("value")).alias("value")]).collect()
    print("max value column:", pa.Table.from_batches(batches).to_pydict())

    batches = df.aggregate([], [f.avg(col("value")).alias("value")]).collect()
    print("mean value column:", pa.Table.from_batches(batches).to_pydict())

    batches = df.aggregate(
        [col("name")], [f.count(col("name")).alias("name_count")]
    ).collect()
    print("name value column:", pa.Table.from_batches(batches).to_pydict())

    print(
        "multiply value by 2:", pc.multiply(tbl.column("value"), 2).to_pylist()
    )

    batches = df.select(
        (col("value") * literal(2)).alias("value_mul_2")
    ).collect()
    print("multiply value by 2:", pa.Table.from_batches(batches).to_pydict())

    batches = df.select(
        (col("value") * col("value")).alias("value_sqr")
    ).collect()
    print(
        "multiply value by itself:", pa.Table.from_batches(batches).to_pydict()
    )

    batches = df.filter(
        (col("value") > literal(2)).alias("value_gt_2")
    ).collect()
    print(
        "rows where value is greater than 2:",
        pa.Table.from_batches(batches).to_pydict(),
    )


def main():
    fire.Fire(
        {
            "metadata": metadata,
            "schema": schema,
            "column_names": column_names,
            "column_info": column_info,
            "column_stats_set": column_stats_set,
            "column_stats": column_stats,
            "duck": duck,
            "ch_local": ch_local,
            "ch_server_184m": ch_server_184m,
            "arrow_parquet_example": arrow_parquet_example,
            "arrow_compute_example": arrow_compute_example,
            "datafusion_parquet": datafusion_parquet,
            "datafusion_compute_example": datafusion_compute_example,
        }
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
