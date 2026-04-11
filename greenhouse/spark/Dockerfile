FROM apache/spark:4.0.1

USER root

RUN apt-get update && \
    apt-get install -y python3-pip && \
    pip3 install --no-cache-dir cassandra-driver && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/project

USER spark
