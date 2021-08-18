# Minio and Clickhouse

## Setup Minio server

1. Start minio server

```
minio server --address :9001 minio-data
```

2. Check the server is running on port 9001

```
ss -a | grep 9001
```

3. View the web UI at http://ip-address:9001/

4. Login with minioadmin, minioadmin

## Use the minio server with minio client mc

1. Setup minio client config

```
mc config host add minio http://localhost:9001 minioadmin minioadmin
```

2. List minio configured hosts

```
mc config host ls
```

3. List minio buckets

```
mc ls minio/
```

4. Create a bucket

```
mc mb minio/first-bucket
```

5. Create another bucket

```
mc mb minio/second-bucket
```

## Use minio server with aws cli

https://docs.min.io/docs/aws-cli-with-minio

1. Setup aws configure with 

```
aws configure
```

2. Use minioadmin as AWS Access Key ID

3. Use minioadmin as AWS Secret Access Key

4. Use us-east-1 as the Default region name

5. Display buckets

```
aws --endpoint-url http://127.0.0.1:9001 s3 ls
```

## From Clickhouse read S3 data

https://clickhouse.tech/docs/en/sql-reference/table-functions/s3/

1. Select from S3 CSV files using clickhouse

```
select *
from s3('http://127.0.0.1:9001/first-bucket/stock-example.csv',
    'minioadmin', 'minioadmin', 'CSV',
    'plant Int16, code Int16, service_level Float32, qty Int8');
```

2. Select from S3 parquet files using clickhouse

```
select *
from s3('http://127.0.0.1:9001/first-bucket/stock-example.parq',
    'minioadmin', 'minioadmin', 'Parquet',
    'plant Int16, code Int16, service_level Float32, qty Int8');
```

## Setup Clickhouse storage configuration

1. sudo vim /etc/clickhouse-server/config.d/storage.xml

```xml
<yandex>
  <storage_configuration>
    <disks>
      <s3>
        <type>s3</type>
        <endpoint>http://127.0.0.1:9001/second-bucket/data/</endpoint>
        <access_key_id>minioadmin</access_key_id>
        <secret_access_key>minioadmin</secret_access_key>
      </s3>
    </disks>
    <policies>
      <s3>
        <volumes>
          <main>
            <disk>s3</disk>
          </main>
        </volumes>
      </s3>
    </policies>
  </storage_configuration>
</yandex>
```

2. Change ownership for configuration file
sudo chown clickhouse:clickhouse /etc/clickhouse-server/config.d/storage.xml

3. Change permission for configuraiton file
sudo chmod 400 /etc/clickhouse-server/config.d/storage.xml

4. Delete old log file
sudo rm /var/log/clickhouse-server/clickhouse-server.err.log

5. Restart clickhouse
sudo service clickhouse-server restart

6. Display any errors
sudo cat /var/log/clickhouse-server/clickhouse-server.err.log

## Use the S3 storage for tables

1. Create a table

```sql
CREATE TABLE visits (
    id UInt64,
    duration Float64,
    url String,
    created DateTime
) ENGINE = MergeTree() 
PRIMARY KEY id 
ORDER BY id
SETTINGS storage_policy='s3';
```

2. Insert data into tables

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

3. Query the table

```sql
SELECT *
FROM visits
```

4. Optimize storage

```sql
optimize table visits
```

5. Query the table again

```sql
select *
from visits
```


