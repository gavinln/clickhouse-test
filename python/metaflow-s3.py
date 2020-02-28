'''
Use metaflow to read and write to S3
'''
import logging
import time

from pathlib import Path

from metaflow import S3

from IPython import embed


logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


SCRIPT_DIR = Path(__file__).parent.resolve()


def main():
    start_time = time.time()
    with S3(s3root='s3://airline-parq/') as s3:
        s3objs = s3.get_all()
        print(len(s3objs))
        # s3obj = s3.get('2008_cleaned.gzip.parq')
        # print('location', s3obj.url)
        # print('key', s3obj.key)
        # print('size', s3obj.size)
        # print('local path', s3obj.path)
        elapsed = time.time() - start_time
        print('Elapsed {}'.format(elapsed))


if __name__ == '__main__':
    main()
