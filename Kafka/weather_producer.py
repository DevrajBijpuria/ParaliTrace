import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
import yaml
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

ROOT = Path(__file__).resolve().parent.parent

TOPIC = "weather.raw"
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"

SEASONS = [("2023-09-15", "2023-12-15"), ("2024-09-15", "2024-12-15"), ("2025-09-15", "2025-12-15"),("2026-09-15","2026-10-08")]   

producer = Producer({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"), "enable.idempotence": True, "linger.ms": 50})
schema_registry = SchemaRegistryClient({"url": os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")})
avro_serializer = AvroSerializer(schema_registry, (ROOT / "schemas" / "weather_raw.avsc").read_text(encoding="utf-8"))
ctx = SerializationContext(TOPIC, MessageField.VALUE)

stats = {"ok": 0, "failed": 0}


def err_report(err, msg):
    if err is not None:
        stats["failed"] += 1
        print(f"Delivery failed for key {msg.key()}: {err}")
    else:
        stats["ok"] += 1


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "backfill"
    if mode not in ("backfill", "live"):
        sys.exit("usage: python weather_producer.py [backfill|live]")

    with open(ROOT / "route_points.yaml", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    points = cfg["route_points"]
    variables = cfg["hourly_variables"]

    base_params = {
        "latitude": ",".join(str(p["lat"]) for p in points),
        "longitude": ",".join(str(p["lon"]) for p in points),
        "hourly": ",".join(variables),
        **cfg["request_params"],
    }

    if mode == "live":
        requests_to_make = [(FORECAST_URL, {"past_days": 2, "forecast_days": 3}, "open_meteo_forecast", "live")]
    else:
        requests_to_make = [(ARCHIVE_URL, {"start_date": s, "end_date": e}, "open_meteo_archive", f"{s}/{e}") for s, e in SEASONS]

    for url, extra, source, window in requests_to_make:
        while True:
            r = requests.get(url, params={**base_params, **extra}, timeout=120)
            if r.status_code == 429:
                print("rate limited, sleeping 60s")
                time.sleep(60)
                continue
            r.raise_for_status()
            break
        data = r.json()
        if isinstance(data, dict):                     
            data = [data]

        for point, block in zip(points, data):        
            hourly = block.get("hourly") or {}
            times = hourly.get("time") or []
            sent = 0
            for i, t in enumerate(times):
                msg = {
                    "point_key": point["point_key"],
                    "requested_lat": float(point["lat"]),
                    "requested_lon": float(point["lon"]),
                    "grid_lat": block.get("latitude"),
                    "grid_lon": block.get("longitude"),
                    "elevation": block.get("elevation"),
                    "time_local": t,
                    "timezone": block.get("timezone"),
                    "utc_offset_seconds": block.get("utc_offset_seconds"),
                    "source": source,
                    "request_window": window,
                    "ingested_at": datetime.now(timezone.utc).isoformat(),
                }
                for var in variables:
                    values = hourly.get(var)
                    msg[var] = float(values[i]) if values and values[i] is not None else None

                value = avro_serializer(msg, ctx)
                while True:
                    try:
                        producer.produce(TOPIC, key=point["point_key"].encode("utf-8"), value=value, callback=err_report)
                        break
                    except BufferError:
                        producer.poll(1)
                producer.poll(0)
                sent += 1
            print(f'{point["point_key"]:<12} {window}: {sent} rows')
        time.sleep(1)

    producer.flush()
    print(f"Done ({mode}). Delivered: {stats['ok']}, failed: {stats['failed']}")