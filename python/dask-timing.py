import logging
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import dask.dataframe as dd

from IPython import embed


logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


SCRIPT_DIR = Path(__file__).parent.resolve()


def get_query(df):
    ''' get avg delay and flight count for airports > 35,000 flights/per month

        About 140 seconds for 123 million rows from 1987 to 2008
    '''
    df2 = df.groupby(['Origin', 'Year', 'Month']).agg({
            'DepDelay': np.mean,
            'Origin': np.size
    }).rename(columns={'Origin': 'count_all'}).compute()
    df3 = df2[df2.count_all > 35000]
    return df3


def main() -> None:
    airline_data_dir = (SCRIPT_DIR / 'temp').resolve()
    airline_files = airline_data_dir.glob('*_cleaned.gzip.parq')
    df = dd.read_parquet(list(airline_files), engine='pyarrow')

    start_time = time.time()
    df2 = get_query(df)
    elapsed = time.time() - start_time
    print(df2)
    print('Elapsed = {:,.2f} seconds'.format(elapsed))


if __name__ == '__main__':
    main()
