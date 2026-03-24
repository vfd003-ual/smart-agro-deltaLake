from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, desc, when, sum
from pyspark.sql.types import StructType, StructField, StringType, BooleanType

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "wikipedia_edits"

def run_spark_batch():
    print("Iniciando sesión de Spark en modo Batch...")
    
    # 1. Inicializamos Spark Session
    spark = SparkSession.builder \
        .appName("WikipediaBatchAnalysis") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1") \
        .getOrCreate()
        
    # Reducimos el nivel de logs (INFO) de Spark para que no ensucie la consola
    spark.sparkContext.setLogLevel("WARN")
    
    # 2. Definimos el esquema del JSON que esperamos recibir
    json_schema = StructType([
        StructField("user", StringType(), True),
        StructField("title", StringType(), True),
        StructField("wiki", StringType(), True),
        StructField("bot", BooleanType(), True)
    ])

    print(f"Conectando a Kafka en {KAFKA_BROKER}, leyendo todo el historial del topic '{KAFKA_TOPIC}'...")

    # 3. Leemos el topic en modo BATCH (read en lugar de readStream)
    # Usamos "earliest" para leer todos los mensajes desde el principio de los tiempos
    kafka_df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    # 4. Transformamos los datos crudos
    parsed_df = kafka_df \
        .selectExpr("CAST(value AS STRING) as json_string", "timestamp") \
        .select(from_json(col("json_string"), json_schema).alias("data"), "timestamp") \
        .select("data.*", "timestamp")

    # 5. Agregación Histórica (Ventanas de 1 hora)
    # Hemos quitado el .withWatermark() porque en Batch tenemos todos los datos estáticos, 
    # por lo que no existe el concepto de "datos tardíos".
    aggregated_df = parsed_df \
        .groupBy(window(col("timestamp"), "1 hour")) \
        .agg(
            sum(when(col("bot") == True, 1).otherwise(0)).alias("bots"),
            sum(when(col("bot") == False, 1).otherwise(0)).alias("humans")
        ) \
        .orderBy(desc("window")) 

    print("Calculando el histórico, esto puede tardar unos segundos...\n")

    # 6. Ejecutamos la acción y mostramos los resultados
    # Al usar .show(), Spark procesa todo de golpe, lo imprime y luego el script termina.
    aggregated_df.show(50, truncate=False)

if __name__ == "__main__":
    run_spark_batch()