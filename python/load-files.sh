#!/bin/bash

for FILENAME in ../clickhouse/airline-data/*.parq; do
    clickhouse-client --query="INSERT INTO flight FORMAT Parquet" < $FILENAME
    # echo $FILENAME
done
