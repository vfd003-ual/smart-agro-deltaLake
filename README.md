# Smart Agro Delta Lake

Proyecto base para la asignatura Infraestructura Big Data, orientado al arranque del TFM.

El flujo implementado cubre la entrega minima solicitada:

- Ingesta: producer Python con 3 modos (simulado de invernaderos de Almeria, CSV historico y API AEMET de Almeria) que publica en Kafka.
- Procesamiento: Spark Structured Streaming con ventanas temporales y agregaciones.
- Almacenamiento: persistencia de metricas agregadas en Cassandra.
- Visualizacion: dashboard de Grafana con paneles sobre Cassandra.
- Infraestructura: todo desplegado con Docker Compose en una unica red.

Ademas, se deja MinIO desplegado para la evolucion del TFM (lakehouse Bronze/Silver/Gold con Delta Lake).

## Arquitectura

1. `greenhouse/producer/producer.py` genera eventos IoT desde simulacion (Almeria), CSV o AEMET (Almeria).
2. Los eventos se publican en el topic Kafka `greenhouse.sensors`.
3. `greenhouse/spark/streaming_job.py` consume desde Kafka con Spark Structured Streaming.
4. Spark aplica watermark + ventana de 5 minutos y calcula agregados por invernadero.
5. Cada microbatch se escribe en Cassandra (`smartagro.window_metrics`).
6. Grafana consulta Cassandra para visualizar tendencias operativas.

## Estructura del repositorio

```text
.
|-- docker-compose.yml
|-- greenhouse
|   |-- cassandra
|   |   `-- init.cql
|   |-- grafana
|   |   `-- dashboard_greenhouse.json
|   |-- producer
|   |   |-- producer.py
|   |   `-- setup_topic.py
|   |-- spark
|   |   |-- Dockerfile
|   |   `-- streaming_job.py
|   `-- requirements.txt
`-- docs
```

## Requisitos

- Docker + Docker Compose.
- Python 3.10+ en host (solo para producer y utilidades locales).

## Despliegue rapido

1. Levantar infraestructura:

```bash
docker compose up -d --build
```

2. Crear topic de Kafka:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r greenhouse/requirements.txt
python greenhouse/producer/setup_topic.py
```

3. Inicializar esquema de Cassandra:

```bash
docker compose exec -T cassandra cqlsh < greenhouse/cassandra/init.cql
```

4. Lanzar job de Spark Streaming:

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit \
	/opt/project/greenhouse/spark/run-streaming.sh
```

5. Iniciar producer (modo simulado):

```bash
python greenhouse/producer/producer.py --events-per-second 2
```

6. Iniciar producer leyendo CSV:

```bash
python greenhouse/producer/producer.py --mode csv --csv-path data/greenhouse_crop_yields.csv --events-per-second 2 --max-events 200
```

7. Iniciar producer con API AEMET:

```bash
cp .env.example .env
# Edita .env y agrega AEMET_API_KEY
python greenhouse/producer/producer.py --mode aemet --events-per-second 0.0033
```

Por defecto se usa `AEMET_STATION_ID=6325O` (Almeria Aeropuerto).

## Grafana

- URL: `http://localhost:3000`
- Usuario: `admin`
- Password: `admin`

Pasos:

1. Instalar datasource Cassandra (ya se instala por variable `GF_INSTALL_PLUGINS`).
2. Crear datasource `Cassandra` apuntando a host `cassandra:9042`, keyspace `smartagro`.
3. Importar dashboard desde `greenhouse/grafana/dashboard_greenhouse.json`.

## Puertos principales

- Kafka broker externo: `9092`
- Redpanda Console: `8090`
- Spark master UI: `8080`
- Spark worker UI: `8081`
- Cassandra: `9042`
- Grafana: `3000`
- MinIO API: `9000`
- MinIO Console: `9001`

## Decisiones de diseno

- Ventanas de 5 minutos para monitorizacion operativa estable y simple de justificar.
- Cassandra como store de lectura rapida para paneles live.
- `foreachBatch` en Spark para controlar escritura y mantener flexibilidad.
- MinIO incluido para el camino de TFM sin afectar la entrega minima de la asignatura.

## Siguientes pasos para TFM

1. Duplicar el stream hacia MinIO como capa Bronze.
2. Crear pipelines batch Silver y Gold con Delta Lake.
3. Entrenar modelos Spark MLlib con datos Gold.
4. Ampliar dashboard con paneles historicos y predicciones.
