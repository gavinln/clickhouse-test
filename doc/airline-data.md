# Airline data

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

### Get airline data


1. Get data in parquet format

```
clickhouse-client -h 10.0.0.2 -q 'select * from flight limit 1000000' -f Parquet > tmp.parq
```

### Materialized view

1. Create year

```
create materialized view flight_view2
engine = AggregatingMergeTree() ORDER BY (Origin, Dest, Year, Month)
populate
as select
    Origin, Dest, Year, Month,
    countState() as count_All
from flight
group by Origin, Dest, Year, Month
```

### Metadata queries

```
select
    table, count(*) as columns,
    sum(data_compressed_bytes) as tc,
    sum(data_uncompressed_bytes) as tu
from system.columns
where database = currentDatabase()
group by table
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

