import argparse
import json
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

TOPIC = "greenhouse.sensors"
GREENHOUSES = ["gh-a", "gh-b", "gh-c"]
ZONES = ["north", "south", "east", "west"]


def build_event(event_id: int) -> dict:
    greenhouse_id = random.choice(GREENHOUSES)
    zone = random.choice(ZONES)

    return {
        "event_id": event_id,
        "sensor_id": f"sensor-{random.randint(1, 80):03d}",
        "greenhouse_id": greenhouse_id,
        "zone": zone,
        "temperature_c": round(random.uniform(17.0, 34.0), 2),
        "humidity_pct": round(random.uniform(38.0, 90.0), 2),
        "co2_ppm": round(random.uniform(350.0, 1400.0), 2),
        "soil_moisture_pct": round(random.uniform(15.0, 70.0), 2),
        "light_lux": round(random.uniform(1000.0, 45000.0), 2),
        "event_ts": datetime.now(timezone.utc).isoformat(),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Greenhouse Kafka producer")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--events-per-second", type=float, default=1.0)
    parser.add_argument("--max-events", type=int, default=0)
    args = parser.parse_args()

    period = 1.0 / args.events_per_second if args.events_per_second > 0 else 1.0

    producer = Producer({"bootstrap.servers": args.bootstrap_servers})

    event_id = 0
    while True:
        event_id += 1
        event = build_event(event_id)

        producer.produce(
            TOPIC,
            key=event["greenhouse_id"],
            value=json.dumps(event),
        )
        producer.poll(0)

        print(
            "sent",
            json.dumps(
                {
                    "event_id": event["event_id"],
                    "greenhouse_id": event["greenhouse_id"],
                    "temperature_c": event["temperature_c"],
                    "humidity_pct": event["humidity_pct"],
                }
            ),
        )

        if args.max_events > 0 and event_id >= args.max_events:
            break

        time.sleep(period)

    producer.flush()


if __name__ == "__main__":
    main()
