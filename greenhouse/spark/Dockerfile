FROM apache/spark:4.0.1

USER root

RUN apt-get update && \
    apt-get install -y python3-pip && \
    pip3 install --no-cache-dir cassandra-driver && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/project

USER spark
# Download Kafka connectors and dependencies

USER root
RUN apt-get update && apt-get install -y curl && \
    curl -s -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-4.0.1.jar https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/4.0.1/spark-sql-kafka-0-10_2.12-4.0.1.jar && \
    curl -s -o /opt/spark/jars/kafka-clients-3.6.1.jar https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.6.1/kafka-clients-3.6.1.jar && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

USER spark
