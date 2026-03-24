-- Para crearlo: docker exec -i cassandra cqlsh < setup_cassandra.cql
-- Para borrarlo: docker exec -it cassandra cqlsh -e "DROP KEYSPACE wikipedia;"

CREATE KEYSPACE IF NOT EXISTS wikipedia
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE wikipedia;

CREATE TABLE IF NOT EXISTS edits_by_window (
    wiki         text,
    window_start timestamp,
    window_end   timestamp,
    bots         bigint,
    humans       bigint,
    PRIMARY KEY (wiki, window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);
