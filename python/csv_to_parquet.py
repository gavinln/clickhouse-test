import logging
import sys
from pathlib import Path

import pandas as pd

from IPython import embed


logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


SCRIPT_DIR = Path(__file__).parent.resolve()


def write_parquet(df, parq_file):
    df.to_parquet(parq_file)


def write_parquet_gzip(df, parq_file):
    df.to_parquet(parq_file, compression='gzip')


def main() -> None:
    print('in csv_to_parquet')
    read_csv_file = (
        SCRIPT_DIR / '..' / 'clickhouse' / 'stock-example.csv').resolve()
    if not read_csv_file.is_file():
        sys.exit('File {} does not exist'.format(read_csv_file))

    col_names = ['plant', 'code', 'service_level', 'qty']
    df = pd.read_csv(read_csv_file, names=col_names)
    write_parq_file = (
        SCRIPT_DIR / '..' / 'clickhouse' / 'stock-example.parq').resolve()
    write_parquet(df, write_parq_file)

    write_parq_gzip_file = (
        SCRIPT_DIR / '..' / 'clickhouse' / 'stock-example.gzip.parq').resolve()
    write_parquet_gzip(df, write_parq_gzip_file)


if __name__ == '__main__':
    main()
