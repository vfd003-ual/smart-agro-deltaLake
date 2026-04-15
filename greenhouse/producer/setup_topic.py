from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP_SERVERS = "localhost:9092"
TOPICS = [
    "greenhouse.sensors.simulated",
    "greenhouse.sensors.csv",
    "greenhouse.sensors.aemet",
]


def main() -> None:
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    metadata = admin.list_topics(timeout=10)

    topics_to_create = [
        NewTopic(topic_name, num_partitions=3, replication_factor=1)
        for topic_name in TOPICS
        if topic_name not in metadata.topics
    ]

    for topic_name in TOPICS:
        if topic_name in metadata.topics:
            print(f"Topic '{topic_name}' already exists")

    if not topics_to_create:
        return

    futures = admin.create_topics(topics_to_create)

    for topic, future in futures.items():
        try:
            future.result()
            print(f"Topic '{topic}' created")
        except Exception as exc:
            print(f"Error creating topic '{topic}': {exc}")


if __name__ == "__main__":
    main()
