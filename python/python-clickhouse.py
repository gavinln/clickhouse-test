'''
Connect to clickhouse using sqlalchemy
'''
import logging
from pathlib import Path

import pandas as pd

from clickhouse_driver import Client

import sqlalchemy as sa


logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


SCRIPT_DIR = Path(__file__).parent.resolve()


def execute_queries():
    client = Client('10.0.0.2')

    print(client.execute('SHOW TABLES'))

    sql = 'select Year, Month, DayofMonth, Origin from flight limit 3;'
    for row in client.execute(sql):
        print(row)


def execute_sqlalchemy():
    engine = sa.create_engine('clickhouse://default@10.0.0.2:8123/default')
    sql = 'select Year, Month, DayofMonth, Origin from flight limit 3'
    with engine.begin() as connection:
        rows = connection.execute(sql)
        for row in rows:
            print(row)


def main() -> None:
    print('in clickhouse-python.py file')
    # execute_queries()
    execute_sqlalchemy()


if __name__ == '__main__':
    main()
