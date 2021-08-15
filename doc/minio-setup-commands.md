# Minio and Clickhouse

## Setup Minio server

1. Start minio server

```
minio server --address :9001 data
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


