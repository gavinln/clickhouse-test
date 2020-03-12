'''
In IPython use
In [1]: %load_ext autoreload
In [2]: %autoreload 2
'''
import logging
from pathlib import Path

import pandas as pd

from IPython import embed

import clickhouse_util as chu


logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


SCRIPT_DIR = Path(__file__).parent.resolve()


def main() -> None:
    print('in main')
    data_file = (SCRIPT_DIR / '..' / '2008_cleaned.gzip.parq').resolve()
    print(data_file)
    df = pd.read_parquet(data_file)
    print(df.meta.shape)
    embed()


if __name__ == '__main__':
    main()
