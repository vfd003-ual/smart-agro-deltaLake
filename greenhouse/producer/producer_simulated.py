import argparse
import time

from confluent_kafka import Producer

try:
    from greenhouse.producer.common import (
        SOURCE_SIMULATED,
        TOPIC_SIMULATED,
        add_source_metadata,
        build_simulated_event,
        publish_event,
        safe_period,
    )
except ModuleNotFoundError:
    from common import (
        SOURCE_SIMULATED,
        TOPIC_SIMULATED,
        add_source_metadata,
        build_simulated_event,
        publish_event,
        safe_period,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Greenhouse simulated producer")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--events-per-second", type=float, default=1.0)
    parser.add_argument("--max-events", type=int, default=0)
    args = parser.parse_args()

    producer = Producer({"bootstrap.servers": args.bootstrap_servers})
    period = safe_period(args.events_per_second)

    event_id = 0
    while True:
        event_id += 1
        event = build_simulated_event(event_id)
        event = add_source_metadata(event, SOURCE_SIMULATED, TOPIC_SIMULATED)
        publish_event(producer, TOPIC_SIMULATED, event)

        if args.max_events > 0 and event_id >= args.max_events:
            break

        time.sleep(period)

    producer.flush()


if __name__ == "__main__":
    main()
