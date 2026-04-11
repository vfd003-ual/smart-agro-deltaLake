#!/bin/bash
/opt/spark/bin/spark-submit \
  --jars /opt/spark/jars/spark-sql-kafka-0-10_2.12-4.0.1.jar,/opt/spark/jars/kafka-clients-3.6.1.jar \
  /opt/project/greenhouse/spark/streaming_job.py
