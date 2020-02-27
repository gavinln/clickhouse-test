# clickhouse-test

* Source code - [Github][1]
* Author - Gavin Noronha - <gavinln@hotmail.com>

[1]: https://github.com/gavinln/clickhouse-test

## About


This project provides a [Ubuntu (18.04)][10] [Vagrant][20] Virtual Machine (VM)
with [Clickhouse][30]. Clickhouse is is an open-source column-oriented DBMS
(columnar database management system) for online analytical processing (OLAP).

[10]: http://releases.ubuntu.com/18.04/
[20]: http://www.vagrantup.com/
[30]: https://clickhouse.tech/

There are [Ansible][90] scripts that automatically install the software when
the VM is started.

[90]: https://www.ansible.com/

## Metadata queries

1. Get list of tables

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
    and table = 'flight'
```


## Setup the machine

All the software installed exceeds the standard 10GB size of the virtual
machine disk. Install the following plugin to resize the disk.

1. List the vagrant plugins

```
vagrant plugin list
```

2. Install the Vagrant [disksize][100] plugin

```
vagrant plugin install vagrant-disksize
```

[100]: https://github.com/sprotheroe/vagrant-disksize


3. Login to the virtual machine

```
vagrant ssh
```

4. Change to the root directory

```
cd /vagrant
```

5. Install clickhouse client


```
sudo apt-key adv --keyserver keyserver.ubuntu.com --recv E0C56BD4
echo "deb http://repo.yandex.ru/clickhouse/deb/stable/ main/" | sudo tee /etc/apt/sources.list.d/clickhouse.list
sudo apt update
sudo apt install -y clickhouse-client
```

6. Start the clickhouse client

```
clickhouse-client -m 
```

### Connect to the server

1. Start the clickhouse-client

```
# -m for multiline mode (end query with semicolon) 
docker exec -it ch-server /usr/bin/clickhouse-client -m 
```

2. List databases

```
show databases;
```

3. Create a database

```
CREATE DATABASE test;
```

4. Use the new database

```
USE test;
```

5. Create a table

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

6. Insert data into tables

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

7. Query the table

```sql
SELECT url, duration
FROM visits
WHERE url = 'http://example2.com' LIMIT 2;
```

9. Run an aggregation query

```sql
SELECT SUM(duration) FROM visits;
```

10. Run a query that returns an array

```sql
SELECT topK(2) (url) FROM visits;
```

11. Drop the table

```sql
DROP table visits;
```

12. Drop the database

```sql
DROP database test;
```

### Insert csv file into table

1. Create the table

```
CREATE TABLE stock (
     plant Int16,
     code Int16,
     service_level Float32,
     qty Int8
) ENGINE = Log
```

2. Load csv data into the table

```
cat stock-example.csv | clickhouse-client --query="INSERT INTO stock FORMAT CSV";
```

3. Display the data

```
select * from stock
```

4. Get number of rows

```
select count(*) from stock
```

### Insert parquet file into table

1. Load parquet data into the table

```
cat stock-example.parq | clickhouse-client --query="INSERT INTO stock FORMAT Parquet";
```

3. Display the data

```
select * from stock
```

## Load airline data

1. Start clickhouse client

```
clickhouse-client
```

2. List tables

```sql
show tables;
```

3. Drop tables if needed

```sql
drop table if exists flight;
drop table if exists flight_view;
```

4. Exit client

```
exit;
```

5. Use a Python file to load data

```
python3 ./python/clickhouse-airline-parquet.py
```

## Run Jupyter notebooks

1. Setup the Python version

```
pipenv --python $(which python3)
```

2. Install libraries

```
pipenv install
```

3. Install Jupyter notebook extensions

```
pipenv run jupyter contrib nbextension install --user
```

4. Go to the Edit menu nbextensions config option to setup plugins

5. Some useful plugins

* Code prettify
* Collapsible Headings
* Comment/Uncomment Hotkey
* ExecuteTime
* Select CodeMirror Keymap
* Table of Contents (2)

6. Run Jupyter notebook

```
make jupyter-nb
```

## Clickhouse client on Ubuntu

1. Add Clickhouse GPG key

```
sudo apt-key adv --keyserver keyserver.ubuntu.com --recv E0C56BD4
```

2. Add repository to APT repository list

```
echo "deb http://repo.yandex.ru/clickhouse/deb/stable/ main/" | sudo tee /etc/apt/sources.list.d/clickhouse.list
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
export CH_SERVER=
clickhouse-client -h $CH_SERVER -d default

alter table flight delete where 1 = 1;

## AWS S3

1. Setup AWS configuration
source ./do_not_checkin/aws-setup.sh




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
