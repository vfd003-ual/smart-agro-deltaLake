# Spark

## Introducción

Apache Spark es un motor de procesamiento de datos distribuido, diseñado para realizar cálculos en paralelo sobre grandes volúmenes de datos. En esta práctica desplegaremos un clúster Spark en modo *standalone* utilizando Docker Compose, conectándolo a la red del clúster Hadoop desplegado en la práctica anterior.

### Arquitectura objetivo

El clúster Spark se compone de los siguientes nodos:

| Contenedor | Rol | Descripción |
|------------|-----|-------------|
| spark-master | Master | Coordina el clúster y asigna tareas a los workers |
| spark-worker-1 | Worker | Nodo de computación que ejecuta las tareas |
| spark-worker-2 | Worker | Nodo de computación que ejecuta las tareas |
| spark-worker-3 | Worker | Nodo de computación que ejecuta las tareas |

Todos los nodos utilizan una imagen personalizada (`spark-custom`) construida a partir de `apache/spark:4.1.1`, que añade Python 3.13 para coincidir con la versión del driver (Jupyter). Los contenedores se conectan a la misma red Docker que el clúster Hadoop, lo que permite a Spark acceder a los datos almacenados en HDFS.

```
┌─────────────────────────────────────────────────────────────────┐
│                      Red: hadoop_hadoopnet                      │
│                       172.28.1.0/24                             │
│                                                                 │
│  ┌──────────────┐                                               │
│  │ spark-master │  ← Puerto 8080 (Web UI)                       │
│  │  172.28.1.10 │  ← Puerto 7077 (Cluster Manager)              │
│  └──────┬───────┘                                               │
│         │  spark://spark-master:7077                            │
│      ┌──┴──────────────┬──────────────┐                         │
│      │                 │              │                         │
│  ┌───┴────────┐ ┌──────┴──────┐ ┌─────┴───────┐                 │
│  │  worker-1  │ │  worker-2   │ │  worker-3   │                 │
│  │ 172.28.1.11│ │ 172.28.1.12 │ │ 172.28.1.13 │                 │
│  └────────────┘ └─────────────┘ └─────────────┘                 │
│                                                                 │
│  ┌────────────────────────────────────────────┐                 │
│  │         Clúster Hadoop (existente)         │                 │
│  │  master · worker1-4 · history · jupyter    │                 │
│  └────────────────────────────────────────────┘                 │
└─────────────────────────────────────────────────────────────────┘
```

## Requisitos previos

- Tener Docker y Docker Compose instalados.
- Tener el clúster Hadoop de la práctica anterior desplegado y en ejecución (`docker compose up` en la carpeta `hadoop`), ya que Spark se conectará a su red.

## Imagen personalizada (`Dockerfile`)

La imagen base `apache/spark:4.1.1` incluye Python 3.10, pero el contenedor Jupyter del clúster Hadoop utiliza Python 3.13. Para que el driver y los executors usen la misma versión, el `Dockerfile` instala Python 3.13 y lo establece como intérprete por defecto mediante las variables `PYSPARK_PYTHON` y `PYSPARK_WORKER_PYTHON`.

## Crear el fichero `compose.yaml`

Dentro de la carpeta `spark`, crea un fichero `compose.yaml` que defina los siguientes servicios:

### Servicio `spark-master`

- **Imagen**: `spark-custom` (construida con `build: .` a partir del `Dockerfile`)
- **Nombre del contenedor**: `spark-master`
- **Red**: `hadoopnet` con IP fija `172.28.1.10`
- **Variables de entorno**:
  - `SPARK_MASTER_HOST=spark-master`
  - `PYSPARK_PYTHON=python3.13`
- **Puertos expuestos**: `8080` (Web UI) y `7077` (Cluster Manager)
- **Comando**: `spark-class org.apache.spark.deploy.master.Master`
- **Reinicio**: `always`

### Servicios `spark-worker-1`, `spark-worker-2` y `spark-worker-3`

- **Imagen**: `spark-custom`
- **Nombres de contenedor**: `spark-worker-1`, `spark-worker-2`, `spark-worker-3`
- **Red**: `hadoopnet` con IPs fijas `172.28.1.11`, `172.28.1.12`, `172.28.1.13`
- **Variables de entorno** (configurables mediante `.env`):
  - `SPARK_WORKER_CORES=${WORKER_CORES}`
  - `SPARK_WORKER_MEMORY=${WORKER_MEMORY}`
- **Puertos expuestos**: `8081`, `8082` y `8083` (mapeados al puerto interno `8081` de cada worker)
- **Comando**: `spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077`
- **Reinicio**: `always`

Los workers dependen del master (`depends_on`) para asegurar que este arranque primero.

### Red

La red `hadoopnet` ya existe, creada por el clúster Hadoop. En el `compose.yaml` de Spark hay que declararla como **red externa** referenciando su nombre real: `hadoop_hadoopnet`.

> **Pista**: Usa la propiedad `external: true` y `name:` en la sección `networks`.

## Arrancar el clúster

1. Asegúrate de que el clúster Hadoop está en ejecución.
2. Arranca el clúster Spark (la primera vez construirá la imagen `spark-custom`):

```bash
cd spark
docker compose up -d
```

3. Verifica que los 4 contenedores están corriendo:

```bash
docker compose ps
```

4. Accede a la interfaz web del Master en: http://localhost:8080

Deberías ver los 3 workers registrados.

## Ejecutar un trabajo de ejemplo

Apache Spark incluye programas de ejemplo en su distribución. Vamos a ejecutar el cálculo de Pi con el algoritmo de Monte Carlo.

Ejecuta el siguiente comando para lanzar el trabajo `SparkPi` distribuyéndolo en el clúster:

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --class org.apache.spark.examples.SparkPi \
  /opt/spark/examples/jars/spark-examples.jar 10000
```

Mientras se ejecuta, abre la interfaz web del Master (http://localhost:8080) y observa cómo aparece la aplicación en la sección *Running Applications*.


## Interfaces web

| Servicio | URL |
|----------|-----|
| Spark Master | http://localhost:8080 |
| Worker 1 | http://localhost:8081 |
| Worker 2 | http://localhost:8082 |
| Worker 3 | http://localhost:8083 |