'''
Inserting data into clickhouse using a child process
DOES NOT WORK

terminating with uncaught exception of type apache::thrift::TException: don't know what type:
'''
import logging
import time
from pathlib import Path
import subprocess
import threading


logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


SCRIPT_DIR = Path(__file__).parent.resolve()


def send_to_process(proc, data_file):
    data_size = 100000
    with data_file.open("rb") as f:
        byte = f.read(data_size)
        while byte != b"":
            byte = f.read(data_size)
            time.sleep(0.1)
            proc.stdin.write(byte)
            proc.stdin.flush()


def output_reader(proc):
    for line in iter(proc.stdout.readline, b''):
        print(line.decode('utf-8'))


def main():
    data_dir = (SCRIPT_DIR / '..' / 'data').resolve()
    data_files = data_dir.glob('*.parq')
    for data_file in sorted(data_files):
        print(data_file)
        break

    ch_client = [
        'clickhouse-client',
        '--echo',
        '-h', '10.0.0.2',
        '-d', 'default',
        '--query=INSERT INTO flight FORMAT Parquet']
    # '--query=select count(*) from flight_view']
    print(' '.join(ch_client))
    proc = subprocess.Popen(
        ch_client, stdin=subprocess.PIPE, stderr=subprocess.STDOUT,
        stdout=subprocess.PIPE)
    t = threading.Thread(target=output_reader, args=(proc,))
    t.start()

    send_to_process(proc, data_file)
    print('closing')
    proc.stdin.flush()
    proc.stdin.close()
    print('waiting')
    try:
        proc.wait()
    except subprocess.TimeoutExpired:
        print('timeout expired')
    print('joining')
    t.join()


if __name__ == '__main__':
    main()
