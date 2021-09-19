# clickhouse-test

* Source code - [Github][1]
* Author - Gavin Noronha - <gavinln@hotmail.com>

[1]: https://github.com/gavinln/clickhouse-test

## About


This project provides a [Ubuntu (20.04)][10] [Vagrant][20] Virtual Machine (VM)
with [Clickhouse][30]. Clickhouse is is an open-source column-oriented DBMS
(columnar database management system) for online analytical processing (OLAP).

[10]: http://releases.ubuntu.com/20.04/
[20]: http://www.vagrantup.com/
[30]: https://clickhouse.tech/

There are [Ansible][90] scripts that automatically install the software when
the VM is started.

[90]: https://www.ansible.com/

## Setup the machine

All the software installed exceeds the standard 10GB size of the virtual
machine disk. Install the following plugin to resize the disk.

1. Check the Vagrantfile

```
vagrant validate
```

2. List the vagrant plugins

```
vagrant plugin list
```

3. Install the Vagrant [vbguest][100] plugin for virtualbox guest

```
vagrant plugin install vagrant-vbguest
```

[100]: https://github.com/dotless-de/vagrant-vbguest

4. Start the virtual machine

```
vagrant up
```

5. Login to the virtual machine

```
vagrant ssh
```

6. Change to the clickhouse directory

```
cd /vagrant/clickhouse
```

7. Start the Clickhouse database

```
sudo service clickhouse-server restart
```

### Example queries

1. Start the clickhouse client

```
clickhouse-client -m 
```

2. List databases

```
show databases;
```

3. Use a database

```
use default
```

4. List tables in database

```
show tables
```

5. Create a database

```
CREATE DATABASE test_example;
```

6. Use the new database

```
USE test_example;
```

7. Create a table

```sql
CREATE TABLE visits (
    id UInt64,
    duration Float64,
    url String,
    created DateTime
) ENGINE = MergeTree() 
PRIMARY KEY id 
ORDER BY id;
```

8. Insert data into tables

```sql
INSERT INTO visits VALUES (1, 10.5, 'http://example.com',
    '2019-01-01 00:01:01');
INSERT INTO visits VALUES (2, 40.2, 'http://example1.com',
    '2019-01-03 10:01:01');
INSERT INTO visits VALUES (3, 13, 'http://example2.com',
    '2019-01-03 12:01:01');
INSERT INTO visits VALUES (4, 2, 'http://example3.com',
    '2019-01-04 02:01:01');
```

9. Query the table

```sql
SELECT url, duration
FROM visits
WHERE url = 'http://example2.com' LIMIT 2;
```

10. Run an aggregation query

```sql
SELECT SUM(duration) FROM visits;
```

11. Run a query that returns an array

```sql
SELECT topK(2) (url) FROM visits;
```

12. Drop the table

```sql
DROP table visits;
```

13. Drop the database

```sql
DROP database test_example;
```

### Insert csv file into table

1. Use database

```
use default;
```

2. Create the table

```
CREATE TABLE stock (
     plant Int16,
     code Int16,
     service_level Float32,
     qty Int8
) ENGINE = MergeTree()
ORDER by plant;
```

3. Load csv data into the table

```
cat stock-example.csv | clickhouse-client --query="INSERT INTO stock FORMAT CSV";
```

4. Display the data

```
select * from stock;
```

5. Get number of rows

```
select count(*) from stock;
```

### Insert parquet file into table

1. Load parquet data into the table

```
cat stock-example.parq | clickhouse-client --query="INSERT INTO stock FORMAT Parquet";
```

2. Display the data

```
select * from stock
```

3. Schedule a merge of parts of a table

```
optimize table stock
```

## Metadata queries

1. List databases

```
show databases;
```

2. Use the system database;

```
use system;
```

3. Get list of tables

```sql
select database, name from tables where database = 'default';
```

2. Get list of columns

```sql
select database, table, name, type from columns where database = 'default';
```

3. Get table and column names

```sql
select database, table, name, type
from columns
where database = 'default'
    and table like 'flight%';
```

## Clickhouse client on Ubuntu

1. Add Clickhouse GPG key

```
sudo apt-key adv --keyserver keyserver.ubuntu.com --recv E0C56BD4
```

2. Add repository to APT repository list

```
echo "deb https://repo.clickhouse.tech/deb/stable/ main/" | sudo tee /etc/apt/sources.list.d/clickhouse.list
```

2. Update packages

```
sudo apt update
```

3.Install clickhouse client

```
sudo apt install clickhouse-client
```

4. Start the clickhouse client

```
export CH_SERVER=
clickhouse-client -h $CH_SERVER -d default -m  # multiline mode
```

## Duck DB

### Duck DB cli

1. Install Duck DB cli

```
wget https://github.com/cwida/duckdb/releases/download/v0.2.1/duckdb_cli-linux-amd64.zip
```

2. Unzip Duck DB cli

```
unzip duckdb_cli-linux-amd64.zip
```

3. Start Duck DB cli saving database in a file

```
./duckdb tmp3-duck.db
```

4.  Create a table by reading from a Parquet file

```
create table flight as select * from parquet_scan('test.parquet');
```

5. Exit cli

```
.quit
```

## AWS command line setup

1. Setup AWS configuration

```
source ./do_not_checkin/s3-user-setup.sh
```

2. Setup AWS completion

```
complete -C aws_completer aws
```

### AWS spot instances

1. Describe spot price history

```
aws ec2 describe-spot-price-history --instance-types t3.2xlarge
```

2. Copy file from S3 - 11 seconds

```
aws s3 cp s3://airline-parq/2008_cleaned.gzip.parq .
```

## Example Parquet commands

### Duckdb parquet

```python
mport duckdb
con = duckdb.connect(database=":memory:")
sql = "select count(*) from parquet_metadata('scripts/ontime-100.pq')"
sql = "select count(*) from parquet_schema('scripts/ontime-100.pq')"
df = con.execute(sql).fetchdf()
```

### Clickhouse parquet

1. Download prepared Clickhouse database partitions for airlines ontime data

```bash
curl -O https://datasets.clickhouse.tech/ontime/partitions/ontime.tar
tar xvf ontime.tar -C /var/lib/clickhouse # path to ClickHouse data directory
# check permissions of unpacked data, fix if required
sudo service clickhouse-server restart
clickhouse-client --query "select count(*) from datasets.ontime"
```

2. Extract data to parquet format

```bash
# select * from datasets.ontime limit 1;
SQL="select * from datasets.ontime limit 100 FORMAT Parquet"
clickhouse-client --query="$SQL" > ontime-100.pq

SQL="select Year, FlightDate, Carrier, FlightNum from datasets.ontime order by Year limit 50000000 FORMAT Parquet"
clickhouse-client --query="$SQL" > scripts/ontime-50m.pq
```

3. Clickhouse local query

```bash
SQL="select Year, count(*) ct from file('scripts/ontime-10m.pq', Parquet, 'Year UInt16, FlightDate Date, Carrier String, FlightNum String') group by Year"
clickhouse-local --query "$SQL"
```

```
Year UInt16, FlightDate Date, Carrier FixedString(2), FlightNum String
```

## Performance duckdb vs clickhouse

Clickhouse is installed in the Vagrant virtual machine directly without using
Docker containers.

1. Login to the VM

```
vagrant ssh
```

2. Setup the Python virtual environment

```
cd /vagrant
pipenv install --skip-lock
pipenv shell
```

3. Measure the performance using a 10 million row dataset

```
python python/parq-cli.py duck scripts/ontime-10m.pq  # 2.6s
python python/parq-cli.py clickhouse scripts/ontime-10m.pq  # 1s
```

4. Measure the performance using a 50 million row dataset

```
python python/parq-cli.py duck scripts/ontime-50m.pq  # 12.5s
python python/parq-cli.py clickhouse scripts/ontime-50m.pq  # 4.4s
```

## Links

* Setup [Clickhouse on Debian][1000]

[1000]: https://www.digitalocean.com/community/tutorials/how-to-install-and-use-clickhouse-on-debian-10

### Python drivers

* Clickhouse [ORM][1010] for Python

[1010]: https://github.com/Infinidat/infi.clickhouse_orm

* Clickhouse to [pandas][1020]

[1020]: https://github.com/kszucs/pandahouse

* Clickhouse [Python driver][1030] with native support

[1030]: https://github.com/mymarilyn/clickhouse-driver

* [Sqlalchemy clickhouse][1040]

[1040]: https://github.com/cloudflare/sqlalchemy-clickhouse

* [Aggregating merge tree]

[1050]: https://www.altinity.com/blog/2020/1/1/clickhouse-cost-efficiency-in-action-analyzing-500-billion-rows-on-an-intel-nuc

### Miscellaneous

* [SqlAlchemy extensions][1060]

[1060]: https://github.com/kvesteri/sqlalchemy-utils

* Python [child processes][1070] buffering

[1070]: https://dzone.com/articles/interacting-with-a-long-running-child-process-in-p

* Python [pipes][1080]

[1080]: https://lyceum-allotments.github.io/series/

* Clickhouse [machine learning][1090]

[1090]: https://presentations.clickhouse.tech/meetup40/ml.pdf

* Clickhouse at [Infovista][1100]

[1100]: https://github.com/ClickHouse/clickhouse-presentations/blob/master/meetup30/infovista.pdf

* Clickhouse at [Messagebird][1110]

[1110]: https://github.com/ClickHouse/clickhouse-presentations/blob/master/meetup20/messagebird.pdf

* Clickhouse [funnel analysis][1120]

[1120]: https://github.com/ClickHouse/clickhouse-presentations/blob/master/meetup33/ximalaya.pdf

* Clickhouse [discussion][1130]

[1130]: https://news.ycombinator.com/item?id=22457767

* Clickhouse [administration][1140]

[1140]: https://www.nedmcclain.com/why-devops-love-clickhouse/

### Clickhouse videos

* Introduction to [Clickhouse][1200]

[1200]: https://www.youtube.com/watch?v=fGG9dApIhDU

### Clickhouse for logging

[Uber engineering blog post][1210]

[1210]: https://eng.uber.com/logging/

### Clickhouse for funnel analysis

[Clickhouse arrays - Part 1][1220]

[1220]: https://altinity.com/blog/harnessing-the-power-of-clickhouse-arrays-part-1

[Clickhouse arrays - Part 2][1230]

[1230] https://altinity.com/blog/harnessing-the-power-of-clickhouse-arrays-part-2

Prepared partitions of [airline ontime data][1240]

[1240]: http://devdoc.net/database/ClickhouseDocs_19.4.1.3-docs/getting_started/example_datasets/ontime/
