import os
from datetime import datetime, timedelta
import random

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, from_unixtime, to_timestamp, unix_timestamp, window
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType
from cassandra.cluster import Cluster

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "smartagro")

schema = StructType(
    [
        StructField("sensor_id", StringType(), False),
        StructField("greenhouse_id", StringType(), False),
        StructField("zone", StringType(), False),
        StructField("temperature_c", DoubleType(), False),
        StructField("humidity_pct", DoubleType(), False),
        StructField("co2_ppm", DoubleType(), False),
        StructField("soil_moisture_pct", DoubleType(), False),
        StructField("light_lux", DoubleType(), False),
        StructField("event_ts", LongType(), False),
    ]
)


def generate_sample_data(num_samples: int = 1000) -> list:
    """Generate sample greenhouse sensor data."""
    greenhouses = ["gh-a", "gh-b", "gh-c"]
    zones = ["north", "south", "east", "west"]
    base_time = datetime.now()
    
    data = []
    for i in range(num_samples):
        ts = int((base_time - timedelta(seconds=num_samples - i)).timestamp() * 1000)
        data.append(
            {
                "sensor_id": f"sensor-{random.randint(1, 80):03d}",
                "greenhouse_id": random.choice(greenhouses),
                "zone": random.choice(zones),
                "temperature_c": round(random.uniform(17.0, 34.0), 2),
                "humidity_pct": round(random.uniform(38.0, 90.0), 2),
                "co2_ppm": round(random.uniform(350.0, 1400.0), 2),
                "soil_moisture_pct": round(random.uniform(15.0, 70.0), 2),
                "light_lux": round(random.uniform(1000.0, 45000.0), 2),
                "event_ts": ts,
            }
        )
    return data


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
    print(f"[Batch {batch_id}] Inserted {len(rows)} window aggregates to Cassandra")


spark = (
    SparkSession.builder.appName("greenhouse-processing-job")
    .master("spark://spark-master:7077")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("Generating sample data...")
sample_data = generate_sample_data(1000)

print("Creating DataFrame...")
df = spark.createDataFrame(sample_data, schema=schema)

print("Applying transformations...")
sensor_events = df.withColumn(
    "event_time", from_unixtime(col("event_ts") / 1000).cast("timestamp")
)

windowed = (
    sensor_events.groupBy(window(col("event_time"), "5 minutes"), col("greenhouse_id"))
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

print("Writing to Cassandra...")
windowed.foreach(lambda row: print(f"  {row.greenhouse_id}: {row.window_start} - avg_temp={row.avg_temperature:.2f}°C, events={row.event_count}"))

rows = windowed.collect()
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
            float(row["avg_temperature"]),
            float(row["avg_humidity"]),
            float(row["avg_co2"]),
            int(row["event_count"]),
        ),
    )

session.shutdown()
cluster.shutdown()

print(f"✓ Wrote {len(rows)} window aggregates to Cassandra")
print("Job completed successfully!")
