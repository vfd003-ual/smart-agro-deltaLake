import os

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, from_json, to_timestamp, window
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:19092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "greenhouse.sensors")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "smartagro")
CHECKPOINT_DIR = os.getenv(
    "CHECKPOINT_DIR", "/opt/project/greenhouse/checkpoints/streaming_job"
)

schema = StructType(
    [
        StructField("event_id", IntegerType(), False),
        StructField("sensor_id", StringType(), False),
        StructField("greenhouse_id", StringType(), False),
        StructField("zone", StringType(), False),
        StructField("temperature_c", DoubleType(), False),
        StructField("humidity_pct", DoubleType(), False),
        StructField("co2_ppm", DoubleType(), False),
        StructField("soil_moisture_pct", DoubleType(), False),
        StructField("light_lux", DoubleType(), False),
        StructField("event_ts", StringType(), False),
    ]
)


def write_batch_to_cassandra(batch_df, batch_id: int) -> None:
    if batch_df.rdd.isEmpty():
        return

    rows = batch_df.collect()
    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect(CASSANDRA_KEYSPACE)

    insert_stmt = session.prepare(
        """
        INSERT INTO window_metrics (
            greenhouse_id,
            window_start,
            window_end,
            avg_temperature,
            avg_humidity,
            avg_co2,
            event_count
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """
    )

    for row in rows:
        session.execute(
            insert_stmt,
            (
                row["greenhouse_id"],
                row["window_start"],
                row["window_end"],
                row["avg_temperature"],
                row["avg_humidity"],
                row["avg_co2"],
                int(row["event_count"]),
            ),
        )

    session.shutdown()
    cluster.shutdown()
    print(f"batch={batch_id} rows={len(rows)} persisted")


spark = (
    SparkSession.builder.appName("greenhouse-streaming-job")
    .master("spark://spark-master:7077")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

raw_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

parsed = raw_stream.selectExpr("CAST(value AS STRING) AS json_payload").select(
    from_json(col("json_payload"), schema).alias("data")
)

sensor_events = parsed.select("data.*").withColumn(
    "event_time", to_timestamp(col("event_ts"))
)

windowed = (
    sensor_events.withWatermark("event_time", "2 minutes")
    .groupBy(window(col("event_time"), "5 minutes"), col("greenhouse_id"))
    .agg(
        avg("temperature_c").alias("avg_temperature"),
        avg("humidity_pct").alias("avg_humidity"),
        avg("co2_ppm").alias("avg_co2"),
        count("*").alias("event_count"),
    )
    .select(
        col("greenhouse_id"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("avg_temperature"),
        col("avg_humidity"),
        col("avg_co2"),
        col("event_count"),
    )
)

query = (
    windowed.writeStream.outputMode("update")
    .option("checkpointLocation", CHECKPOINT_DIR)
    .foreachBatch(write_batch_to_cassandra)
    .start()
)

query.awaitTermination()
