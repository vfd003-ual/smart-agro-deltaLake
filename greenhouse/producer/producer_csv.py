import argparse
import time

from confluent_kafka import Producer

try:
    from greenhouse.producer.common import (
        SOURCE_CSV,
        TOPIC_CSV,
        add_source_metadata,
        csv_events,
        publish_event,
        safe_period,
    )
except ModuleNotFoundError:
    from common import (
        SOURCE_CSV,
        TOPIC_CSV,
        add_source_metadata,
        csv_events,
        publish_event,
        safe_period,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Greenhouse CSV producer")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--events-per-second", type=float, default=1.0)
    parser.add_argument("--max-events", type=int, default=0)
    parser.add_argument("--csv-path", default="data/greenhouse_crop_yields.csv")
    args = parser.parse_args()

    producer = Producer({"bootstrap.servers": args.bootstrap_servers})
    period = safe_period(args.events_per_second)

    sent = 0
    for event in csv_events(args.csv_path):
        enriched_event = add_source_metadata(event, SOURCE_CSV, TOPIC_CSV)
        publish_event(producer, TOPIC_CSV, enriched_event)
        sent += 1

        if args.max_events > 0 and sent >= args.max_events:
            break

        time.sleep(period)

    producer.flush()


if __name__ == "__main__":
    main()
