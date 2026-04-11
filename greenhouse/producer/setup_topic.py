from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "greenhouse.sensors"


def main() -> None:
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    metadata = admin.list_topics(timeout=10)

    if TOPIC in metadata.topics:
        print(f"Topic '{TOPIC}' already exists")
        return

    new_topic = NewTopic(TOPIC, num_partitions=3, replication_factor=1)
    futures = admin.create_topics([new_topic])

    for topic, future in futures.items():
        try:
            future.result()
            print(f"Topic '{topic}' created")
        except Exception as exc:
            print(f"Error creating topic '{topic}': {exc}")


if __name__ == "__main__":
    main()
