import argparse
import os
import time

from confluent_kafka import Producer
from dotenv import load_dotenv

try:
    from greenhouse.producer.common import (
        SOURCE_AEMET,
        TOPIC_AEMET,
        add_source_metadata,
        fetch_latest_aemet_observation,
        publish_event,
        safe_period,
        start_event_id,
    )
except ModuleNotFoundError:
    from common import (
        SOURCE_AEMET,
        TOPIC_AEMET,
        add_source_metadata,
        fetch_latest_aemet_observation,
        publish_event,
        safe_period,
        start_event_id,
    )


def resolve_api_key() -> str:
    api_key = os.getenv("AEMET_API_KEY", "")
    if not api_key:
        raise RuntimeError("AEMET_API_KEY no esta definido en .env")
    return api_key


def main() -> None:
    load_dotenv()

    parser = argparse.ArgumentParser(description="Greenhouse AEMET producer")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--events-per-second", type=float, default=0.0033)
    parser.add_argument("--max-events", type=int, default=0)
    parser.add_argument("--aemet-station", default=os.getenv("AEMET_STATION_ID", "6325O"))
    args = parser.parse_args()

    api_key = resolve_api_key()
    producer = Producer({"bootstrap.servers": args.bootstrap_servers})
    period = safe_period(args.events_per_second)
    retry_sleep = max(5.0, min(60.0, period))

    event_id = start_event_id()
    sent = 0

    while True:
        try:
            event_id += 1
            event = fetch_latest_aemet_observation(api_key, args.aemet_station, event_id)
            event = add_source_metadata(event, SOURCE_AEMET, TOPIC_AEMET)
            publish_event(producer, TOPIC_AEMET, event)
            sent += 1
        except Exception as exc:  # Keep producer alive across transient API/network failures.
            print(f"warning: error obteniendo/publicando AEMET: {exc}")
            time.sleep(retry_sleep)
            continue

        if args.max_events > 0 and sent >= args.max_events:
            break

        time.sleep(period)

    producer.flush()


if __name__ == "__main__":
    main()
