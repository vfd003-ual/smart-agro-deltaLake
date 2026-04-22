import csv
import json
import os
import random
import time
from datetime import datetime, timezone
from typing import Iterator

import requests
from confluent_kafka import Producer

TOPIC_SIMULATED = "greenhouse.sensors.simulated"
TOPIC_CSV = "greenhouse.sensors.csv"
TOPIC_AEMET = "greenhouse.sensors.aemet"

SOURCE_SIMULATED = "simulated"
SOURCE_CSV = "csv"
SOURCE_AEMET = "aemet"

GREENHOUSES = ["alm-poniente", "alm-nijar", "alm-levante"]
ZONES = ["el-ejido", "roquetas", "nijar", "adra"]
AEMET_OPEN_DATA_BASE = "https://opendata.aemet.es/opendata/api"
AEMET_GREENHOUSE_ID = os.getenv("AEMET_GREENHOUSE_ID", "alm-poniente")


def add_source_metadata(event: dict, source_name: str, source_topic: str) -> dict:
    enriched = dict(event)
    enriched["data_source"] = source_name
    enriched["source_topic"] = source_topic
    return enriched


def build_simulated_event(event_id: int) -> dict:
    greenhouse_id = random.choice(GREENHOUSES)
    zone = random.choice(ZONES)

    return {
        "event_id": event_id,
        "sensor_id": f"sensor-{random.randint(1, 80):03d}",
        "greenhouse_id": greenhouse_id,
        "zone": zone,
        "temperature_c": round(random.uniform(18.0, 38.0), 2),
        "humidity_pct": round(random.uniform(30.0, 80.0), 2),
        "co2_ppm": round(random.uniform(380.0, 1300.0), 2),
        "soil_moisture_pct": round(random.uniform(10.0, 65.0), 2),
        "light_lux": round(random.uniform(5000.0, 60000.0), 2),
        "event_ts": datetime.now(timezone.utc).isoformat(),
    }


def map_crop_row_to_event(event_id: int, row: dict) -> dict:
    raw_gh = str(row.get("greenhouse_id", "1")).split(".")[0]
    greenhouse_num = max(1, int(raw_gh or 1))
    greenhouse_id = GREENHOUSES[(greenhouse_num - 1) % len(GREENHOUSES)]

    avg_temp = row.get("avg_temperature_C")
    humidity = row.get("humidity_percent")
    co2 = row.get("co2_ppm")
    light = row.get("light_intensity_lux")

    temperature_c = float(avg_temp) if avg_temp else round(random.uniform(17.0, 34.0), 2)
    humidity_pct = float(humidity) if humidity else round(random.uniform(38.0, 90.0), 2)
    co2_ppm = float(co2) if co2 else round(random.uniform(350.0, 1400.0), 2)
    light_lux = float(light) if light else round(random.uniform(1000.0, 45000.0), 2)

    irrigation = row.get("irrigation_mm")
    soil_moisture_pct = (
        max(5.0, min(90.0, float(irrigation) * 6.0))
        if irrigation
        else round(random.uniform(15.0, 70.0), 2)
    )

    return {
        "event_id": event_id,
        "sensor_id": f"crop-{row.get('crop_type', 'sensor').lower()}-{event_id:04d}",
        "greenhouse_id": greenhouse_id,
        "zone": random.choice(ZONES),
        "temperature_c": round(temperature_c, 2),
        "humidity_pct": round(humidity_pct, 2),
        "co2_ppm": round(co2_ppm, 2),
        "soil_moisture_pct": round(soil_moisture_pct, 2),
        "light_lux": round(light_lux, 2),
        "event_ts": datetime.now(timezone.utc).isoformat(),
    }


def csv_events(csv_path: str) -> Iterator[dict]:
    with open(csv_path, newline="", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)
        event_id = 0
        for row in reader:
            event_id += 1
            yield map_crop_row_to_event(event_id, row)


def pick_float(payload: dict, keys: list[str]) -> float | None:
    for key in keys:
        value = payload.get(key)
        if value is None:
            continue
        if isinstance(value, str):
            value = value.replace(",", ".")
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def fetch_latest_aemet_observation(api_key: str, station_id: str, event_id: int) -> dict:
    metadata_url = (
        f"{AEMET_OPEN_DATA_BASE}/observacion/convencional/datos/estacion/{station_id}"
    )
    metadata_response = requests.get(
        metadata_url, params={"api_key": api_key}, timeout=20
    )
    metadata_response.raise_for_status()
    metadata_payload = metadata_response.json()

    data_url = metadata_payload.get("datos")
    if not data_url:
        raise RuntimeError("AEMET no devolvio URL de datos")

    data_response = requests.get(data_url, timeout=30)
    data_response.raise_for_status()
    observations = data_response.json()

    if not observations:
        raise RuntimeError("AEMET devolvio una lista vacia de observaciones")

    latest = observations[-1]
    temperature_c = pick_float(latest, ["ta", "t", "tamax", "tpr"])
    humidity_pct = pick_float(latest, ["hr", "h", "hrMedia"])
    light_lux = pick_float(latest, ["sol", "vis"])

    greenhouse_id = (
        AEMET_GREENHOUSE_ID if AEMET_GREENHOUSE_ID in GREENHOUSES else GREENHOUSES[0]
    )

    event = {
        "event_id": event_id,
        "sensor_id": f"aemet-{station_id}",
        "greenhouse_id": greenhouse_id,
        "zone": "almeria-outdoor",
        "temperature_c": round(temperature_c if temperature_c is not None else 22.0, 2),
        "humidity_pct": round(humidity_pct if humidity_pct is not None else 65.0, 2),
        "co2_ppm": round(random.uniform(380.0, 520.0), 2),
        "soil_moisture_pct": round(random.uniform(20.0, 55.0), 2),
        "light_lux": round((light_lux if light_lux is not None else 12000.0) * 100.0, 2),
        "event_ts": datetime.now(timezone.utc).isoformat(),
    }
    return event


def publish_event(producer: Producer, topic: str, event: dict) -> None:
    producer.produce(topic, key=event["greenhouse_id"], value=json.dumps(event))
    producer.poll(0)

    print(
        "sent",
        json.dumps(
            {
                "topic": topic,
                "event_id": event["event_id"],
                "data_source": event["data_source"],
                "greenhouse_id": event["greenhouse_id"],
                "temperature_c": event["temperature_c"],
                "humidity_pct": event["humidity_pct"],
            }
        ),
    )


def safe_period(events_per_second: float) -> float:
    return 1.0 / events_per_second if events_per_second > 0 else 1.0


def start_event_id() -> int:
    return int(time.time())
