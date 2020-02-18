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

## Load airline data

1. Start clickhouse client

```
clickhouse-client
```

2. Drop table

```sql
drop table flight;
```

3. Create table

```sql
create table flight (
    Year              Int16,
    Month             Int8,
    DayofMonth        Int16,
    DayOfWeek         Int8,
    DepTime           Nullable(Int16),
    CRSDepTime        Int16,
    ArrTime           Nullable(Int16),
    CRSArrTime        Int16,
    UniqueCarrier     String,
    FlightNum         Int32,
    TailNum           Nullable(String),
    ActualElapsedTime Nullable(Int32),
    CRSElapsedTime    Nullable(Int32),
    AirTime           Nullable(Int32),
    ArrDelay          Nullable(Int32),
    DepDelay          Nullable(Int32),
    Origin            String,
    Dest              String,
    Distance          Nullable(Int32),
    TaxiIn            Nullable(Int32),
    TaxiOut           Nullable(Int32),
    Cancelled         Int8,
    CancellationCode  Nullable(String),
    Diverted          Int8,
    CarrierDelay      Nullable(Int32),
    WeatherDelay      Nullable(Int32),
    NASDelay          Nullable(Int32),
    SecurityDelay     Nullable(Int32),
    LateAircraftDelay Nullable(Int32)
) ENGINE = Log;
```

4. Exit client

```
exit;
```

5. Insert data into tables

```
cat 1987_cleaned.gzip.parq | clickhouse-client --query="INSERT INTO flight FORMAT Parquet"
cat 1988_cleaned.gzip.parq | clickhouse-client --query="INSERT INTO flight FORMAT Parquet"
cat 1989_cleaned.gzip.parq | clickhouse-client --query="INSERT INTO flight FORMAT Parquet"
cat 1990_cleaned.gzip.parq | clickhouse-client --query="INSERT INTO flight FORMAT Parquet"
cat 1991_cleaned.gzip.parq | clickhouse-client --query="INSERT INTO flight FORMAT Parquet"
cat 1992_cleaned.gzip.parq | clickhouse-client --query="INSERT INTO flight FORMAT Parquet"
cat 1993_cleaned.gzip.parq | clickhouse-client --query="INSERT INTO flight FORMAT Parquet"
cat 1994_cleaned.gzip.parq | clickhouse-client --query="INSERT INTO flight FORMAT Parquet"
cat 1995_cleaned.gzip.parq | clickhouse-client --query="INSERT INTO flight FORMAT Parquet"
cat 1996_cleaned.gzip.parq | clickhouse-client --query="INSERT INTO flight FORMAT Parquet"
cat 1997_cleaned.gzip.parq | clickhouse-client --query="INSERT INTO flight FORMAT Parquet"
cat 1998_cleaned.gzip.parq | clickhouse-client --query="INSERT INTO flight FORMAT Parquet"
cat 1999_cleaned.gzip.parq | clickhouse-client --query="INSERT INTO flight FORMAT Parquet"
cat 2000_cleaned.gzip.parq | clickhouse-client --query="INSERT INTO flight FORMAT Parquet"
cat 2001_cleaned.gzip.parq | clickhouse-client --query="INSERT INTO flight FORMAT Parquet"
cat 2002_cleaned.gzip.parq | clickhouse-client --query="INSERT INTO flight FORMAT Parquet"
cat 2003_cleaned.gzip.parq | clickhouse-client --query="INSERT INTO flight FORMAT Parquet"
cat 2004_cleaned.gzip.parq | clickhouse-client --query="INSERT INTO flight FORMAT Parquet"
cat 2005_cleaned.gzip.parq | clickhouse-client --query="INSERT INTO flight FORMAT Parquet"
cat 2006_cleaned.gzip.parq | clickhouse-client --query="INSERT INTO flight FORMAT Parquet"
cat 2007_cleaned.gzip.parq | clickhouse-client --query="INSERT INTO flight FORMAT Parquet"
cat 2008_cleaned.gzip.parq | clickhouse-client --query="INSERT INTO flight FORMAT Parquet"
```
