import logging
from pathlib import Path

import pandas as pd


logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


SCRIPT_DIR = Path(__file__).parent.resolve()


def write_parquet(df, parq_file):
    df.to_parquet(parq_file)


def write_parquet_gzip(df, parq_file):
    df.to_parquet(parq_file, compression='gzip')


def main() -> None:
    parq_file_dir = (
        SCRIPT_DIR / '..' / 'clickhouse').resolve()
    parq_files = parq_file_dir.glob('*_cleaned.gzip.parq')

    dtypes = []
    for parq_file in sorted(parq_files):
        print('processing {}'.format(parq_file.name))
        df = pd.read_parquet(parq_file)
        dtypes.append(df.dtypes)

    dtypes_frame = pd.concat(dtypes, axis=1)

    def combine_types(srs):
        return set(srs.to_list())

    dtypes_srs = dtypes_frame.apply(combine_types, axis=1)
    print(dtypes_srs)


if __name__ == '__main__':
    main()
