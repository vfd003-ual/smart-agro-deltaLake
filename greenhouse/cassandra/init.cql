CREATE KEYSPACE IF NOT EXISTS smartagro
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS smartagro.window_metrics (
  greenhouse_id text,
  window_start timestamp,
  window_end timestamp,
  avg_temperature double,
  avg_humidity double,
  avg_co2 double,
  event_count bigint,
  PRIMARY KEY ((greenhouse_id), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);
