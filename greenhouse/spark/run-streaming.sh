#!/bin/bash
set -e

export HOME=/tmp
mkdir -p /tmp/.ivy2 /tmp/.coursier
export SPARK_SUBMIT_OPTS="-Duser.home=/tmp"

/opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 \
  /opt/project/greenhouse/spark/streaming_job.py
