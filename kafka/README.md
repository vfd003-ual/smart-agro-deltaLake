# Kafka

- **Apache Kafka** (`apache/kafka-native:4.2.0`) — Broker en modo KRaft (sin Zookeeper), actuando como broker y controller en un único nodo.
- **Redpanda Console** (`redpandadata/console:v3.5.3`) — Interfaz web para monitorizar y gestionar el clúster de Kafka.

### Puertos

| Servicio | Puerto |
|---|---|
| Kafka | `9092` |
| Redpanda Console | `8090` |

Kafka utiliza internamente listeners separados para la comunicación entre servicios (`19092`), conexiones externas (`9092`) y el controller (`9093`).

Ambos servicios están conectados a la red `hadoop_hadoopnet`.

### Puesta en marcha

```bash
docker compose up -d
```
