from datetime import timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, desc, when, sum
from pyspark.sql.types import StructType, StructField, StringType, BooleanType
from cassandra.cluster import Cluster

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "wikipedia_edits"
CASSANDRA_HOST = "localhost"
CASSANDRA_KEYSPACE = "wikipedia"
CASSANDRA_TABLE = "edits_by_window"

_cassandra_session = None

def get_cassandra_session():
    global _cassandra_session
    if _cassandra_session is None or _cassandra_session.is_shutdown:
        cluster = Cluster([CASSANDRA_HOST])
        _cassandra_session = cluster.connect(CASSANDRA_KEYSPACE)
    return _cassandra_session

def process_batch(df, epoch_id):
    df.orderBy(desc("window")).show(truncate=False)

    session = get_cassandra_session()
    insert_stmt = session.prepare(
        f"INSERT INTO {CASSANDRA_TABLE} (wiki, window_start, window_end, bots, humans) "
        "VALUES (?, ?, ?, ?, ?)"
    )
    for row in df.collect():
        session.execute(insert_stmt, (
            "wikipedia",
            row["window"]["start"].astimezone(timezone.utc),
            row["window"]["end"].astimezone(timezone.utc),
            int(row["bots"]),
            int(row["humans"]),
        ))

def run_spark_streaming():
    print("Iniciando sesión de Spark...")
    
    # 1. Inicializamos Spark Session
    # El config 'spark.jars.packages' es vital para descargar el conector de Kafka.
    # IMPORTANTE: Cambia '4.1.1' por tu versión exacta de Spark si es diferente.
    spark = SparkSession.builder \
        .appName("WikipediaStreamingAnalysis") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()
        
    # Reducimos el nivel de logs (INFO) de Spark para que no ensucie la consola
    spark.sparkContext.setLogLevel("WARN")
    
    # 2. Definimos el esquema del JSON que esperamos recibir.
    # Spark ignorará los campos del JSON original que no declaremos aquí.
    json_schema = StructType([
        StructField("user", StringType(), True),
        StructField("title", StringType(), True),
        StructField("wiki", StringType(), True),
        StructField("bot", BooleanType(), True)
    ])

    print(f"Conectando a Kafka en {KAFKA_BROKER}, leyendo el topic '{KAFKA_TOPIC}'...")

    # 3. Leemos el flujo de datos (Stream) desde Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # 4. Transformamos los datos crudos
    # Kafka nos da una columna 'value' en formato binario y una columna 'timestamp'.
    # Convertimos 'value' a String y luego parseamos el JSON usando nuestro esquema.
    parsed_df = kafka_df \
        .selectExpr("CAST(value AS STRING) as json_string", "timestamp") \
        .select(from_json(col("json_string"), json_schema).alias("data"), "timestamp") \
        .select("data.*", "timestamp")

    # 5. Agregación en tiempo real (Windowing)
    # Contamos las ediciones agrupando por ventanas de 30 segundos y si es bot o no.
    # Nota: orderBy no está soportado en streaming DFs; lo aplicamos dentro de foreachBatch.
    aggregated_df = parsed_df \
        .withWatermark("timestamp", "10 seconds") \
        .groupBy(window(col("timestamp"), "30 seconds")) \
        .agg(
            sum(when(col("bot") == True, 1).otherwise(0)).alias("bots"),
            sum(when(col("bot") == False, 1).otherwise(0)).alias("humans")
        )

    print("¡Procesamiento en tiempo real iniciado! Esperando el primer lote de datos...\n")

    # 6. Escribimos cada micro-batch en la consola y en Cassandra
    # outputMode "update" solo emite las ventanas modificadas, ideal para Cassandra (upsert nativo).
    # Alternativas: "complete" re-escribe todo (útil para debug), "append" espera a que cierre la ventana.
    query = aggregated_df.writeStream \
        .outputMode("update") \
        .foreachBatch(process_batch) \
        .start()

    # Mantenemos el proceso vivo hasta que se interrumpa manualmente
    query.awaitTermination()

if __name__ == "__main__":
    run_spark_streaming()