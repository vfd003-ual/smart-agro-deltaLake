# Wikipedia Edits: Kafka → Spark Streaming → Cassandra → Grafana

Pipeline en tiempo real que cuenta las ediciones de Wikipedia (bots vs humanos) por ventanas de 30 segundos
y almacena los resultados en Cassandra para su visualización en Grafana.

## Arquitectura

```
wikipedia EventStream  →  Kafka (wikipedia_edits)  →  Spark Streaming
                                                              ↓
                                                        Cassandra  →  Grafana
```

## Requisitos

Instala todas las dependencias:

```bash
pip install -r examples/requirements.txt
```

## Puesta en marcha

### 1- Levantar la infraestructura

```bash
cd /home/ubuntu/ibd
docker compose -f kafka/compose.yaml up -d
docker compose -f cassandra/compose.yaml up -d
docker compose -f grafana/compose.yaml up -d
```

### 2. Crear el esquema en Cassandra

```bash
cd examples/wikipedia
docker exec -i cassandra cqlsh < setup_cassandra.cql
```

### 3. Ejecutar el productor y el consumidor

```bash
# Terminal 1: Envía ediciones de Wikipedia a Kafka
python3 kafka_producer.py

# Terminal 2: Job de Spark Streaming (escribe en consola y en Cassandra)
python3 spark_consumer_streaming.py
```

## 4. Dashboard en Grafana

### 4.1. Configurar el datasource

1. Abre Grafana en http://localhost:3000 (admin / grafana)
2. Ve a **Connections → Add new datasource → Apache Cassandra**
3. Rellena los campos:
   - **Host**: `cassandra`
   - **Port**: `9042`
   - **Keyspace**: `wikipedia`
4. Pulsa **Save & test**

### 4.2. Crear el dashboard y los paneles

1. Ve a **Dashboards → New dashboard → Add visualization**
2. Selecciona el datasource **Cassandra** que acabas de crear
3. Cambia la visualización a **Time series**

Añade **dos queries** (una por métrica), con los siguientes valores:

#### Query A — Bots

| Campo           | Valor            |
|-----------------|------------------|
| Keyspace        | `wikipedia`      |
| Table           | `edits_by_window`|
| Time Column     | `window_start`   |
| Value Column    | `bots`           |
| ID Column       | `wiki`           |
| ID Value        | `wikipedia`      |
| Alias           | `Bots`           |
| Instant         | desactivado      |
| Allow filtering | desactivado      |

#### Query B — Humans

Pulsa **+ Add query** y repite con:

| Campo           | Valor            |
|-----------------|------------------|
| Keyspace        | `wikipedia`      |
| Table           | `edits_by_window`|
| Time Column     | `window_start`   |
| Value Column    | `humans`         |
| ID Column       | `wiki`           |
| ID Value        | `wikipedia`      |
| Alias           | `Humans`         |
| Instant         | desactivado      |
| Allow filtering | desactivado      |

> **Nota sobre ID Column / ID Value:** la columna `wiki` es la clave de partición de la tabla.
> Todas las filas se insertan con `wiki = 'wikipedia'`, lo que permite al plugin de Cassandra en Grafana
> filtrar por partición y ejecutar la query correctamente.
>
> `Instant` debe estar **desactivado** para ver la serie temporal completa en lugar de solo el último valor.

4. Pulsa **Apply** y guarda el dashboard.

### 4.3. Importar el dashboard desde JSON (alternativa)

En lugar de crear los paneles manualmente, puedes importar el dashboard preconstruido incluido en este repositorio:

1. Ve a **Dashboards → New → Import**.
2. Pulsa **Upload dashboard JSON file** y selecciona el archivo `grafana_dashboard_wikipedia.json`.
3. En el desplegable del datasource, selecciona el datasource **Apache Cassandra** que configuraste en el paso 4.1.
4. Pulsa **Import**.

El dashboard incluye cuatro paneles:
- **Último resultado de humanos vs bots** — gráfico de tarta con la última ventana de ediciones por bots y humanos.
- **Histórico de humanos vs bots** — serie temporal con el histórico completo de ediciones por bots y humanos.
- **Último resultado de cambios por idioma** — gráfico de tarta con la última ventana de ediciones por idioma (español, inglés, portugués, alemán, ruso y chino).
- **Histórico de cambios por idioma** — serie temporal con el histórico de ediciones por idioma.
