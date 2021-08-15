# Minio and Clickhouse

## Setup Minio server

1. Start minio server
minio server --address :9001 data

2. Check the server is running on port 9001
ss -a | grep 9001

3. Setup minio client config
mc config host add minio http://localhost:9001 minioadmin minioadmin

4. List minio configured hosts
mc config host ls

5. List minio buckets
mc ls minio/
