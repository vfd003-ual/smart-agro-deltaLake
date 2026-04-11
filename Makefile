PYTHON ?= python3

.PHONY: up down logs init-topic init-cassandra run-spark run-producer clean

up:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f --tail=120

init-topic:
	$(PYTHON) greenhouse/producer/setup_topic.py

init-cassandra:
	docker compose exec -T cassandra cqlsh < greenhouse/cassandra/init.cql

run-spark:
	docker compose exec spark-master /opt/spark/bin/spark-submit /opt/project/greenhouse/spark/streaming_job.py

run-producer:
	$(PYTHON) greenhouse/producer/producer.py --events-per-second 2

clean:
	docker compose down -v
