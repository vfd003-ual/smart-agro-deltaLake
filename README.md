# Infraestructura Big Data

## Componentes

- [Hadoop](hadoop/) — Clúster Hadoop (HDFS + YARN + MapReduce + Jupyter).
- [Spark](spark/) — Clúster Spark.
- [Kafka](kafka/) — Broker Kafka y Redpanda Console.
- [Cassandra](cassandra/) — Base de datos NoSQL Apache Cassandra con interfaz web.
- [Postgres](postgres/) — Base de datos TimescaleDB (PostgreSQL) con PostGIS y pgAdmin.
- [Grafana](grafana/) — Panel de visualización Grafana.

## Herramientas auxiliares

- [Firefox](firefox/) — Navegador Firefox accesible de forma remota vía contenedor.
- [Webtop](webtop/) — Escritorio Linux completo accesible desde el navegador.
- [Scripts](scripts/) — Scripts de utilidad.

## Ejemplos

- [ISS](examples/iss/) — Seguimiento en tiempo real de la Estación Espacial Internacional con Kafka y WebSockets.
- [Ecommerce](examples/ecommerce/) — Demo de Kafka: particiones, grupos de consumidores, escalado horizontal y replay de mensajes.
- [Wikipedia](examples/wikipedia/) — Contador en tiempo real de ediciones de Wikipedia procesadas con Spark Streaming y Cassandra.
- [Gasolineras](examples/gasolineras/) — Pipeline de datos de precios de gasolineras españolas con PostgreSQL y consultas geoespaciales.
