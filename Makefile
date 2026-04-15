PYTHON ?= python3

.PHONY: up down logs init-topic init-cassandra run-spark run-producer-simulated run-producer-csv run-producer-aemet clean

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
	docker compose exec spark-master /opt/project/greenhouse/spark/run-streaming.sh

run-producer-simulated:
	$(PYTHON) greenhouse/producer/producer_simulated.py --events-per-second 2

run-producer-csv:
	$(PYTHON) greenhouse/producer/producer_csv.py --csv-path data/greenhouse_crop_yields.csv --events-per-second 2 --max-events 200

run-producer-aemet:
	$(PYTHON) greenhouse/producer/producer_aemet.py --events-per-second 0.0033

clean:
	docker compose down -v
