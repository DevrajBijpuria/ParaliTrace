import csv
import io
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

load_dotenv()
ROOT = Path(__file__).resolve().parent.parent

producer = Producer({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
                     "enable.idempotence": True, "linger.ms": 50})
schema_registry = SchemaRegistryClient({"url": os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")})
avro_serializer = AvroSerializer(schema_registry, (ROOT / "schemas" / "fires_raw.avsc").read_text(encoding="utf-8"))
TOPIC_1 = "fires.raw"

DATA_DIR = ROOT / "data"
FIRMS_URL = "https://firms.modaps.eosdis.nasa.gov/api/area/csv/{key}/{source}/{area}/{days}"
LIVE_SOURCES = ["VIIRS_SNPP_NRT", "VIIRS_NOAA20_NRT", "VIIRS_NOAA21_NRT"]
LIVE_AREA = "73.5,27.5,78.0,33.0"          
LIVE_DAYS = 2

RENAME = {"brightness": "bright_ti4", "bright_t31": "bright_ti5"}
SCHEMA_FIELDS = ["latitude", "longitude", "bright_ti4", "scan", "track", "acq_date", "acq_time",
                 "satellite", "instrument", "confidence", "version", "bright_ti5", "frp", "daynight", "type"]
stats = {"ok": 0, "failed": 0}


def err_report(err, msg):
    if err is not None:
        stats["failed"] += 1
        print(f"There occurred this {err}")
    else:
        stats["ok"] += 1


def send_row(row, source, source_file):
    renamed = {RENAME.get(k.strip(), k.strip()): v for k, v in row.items()}
    msg = {field: (renamed.get(field) or None) for field in SCHEMA_FIELDS}
    msg["source"] = source
    msg["source_file"] = source_file
    msg["ingested_at"] = datetime.now(timezone.utc).isoformat()

    try:
        key = f"{round(float(renamed['latitude']), 1)}_{round(float(renamed['longitude']), 1)}"
    except (ValueError, KeyError, TypeError):
        key = "invalid"
    value = avro_serializer(msg, SerializationContext(TOPIC_1, MessageField.VALUE))
    while True:
        try:
            producer.produce(TOPIC_1, key=key.encode("utf-8"), value=value, callback=err_report)
            break
        except BufferError:
            producer.poll(1)
    producer.poll(0)


def read_fires_raw():
    for year in ["2023", "2024", "2025"]:
        folder = DATA_DIR / year
        if not folder.is_dir():
            print(f"Folder not found: {folder}")
            continue
        for file in sorted(folder.glob("*.csv")):
            sent = 0
            with file.open("r", newline="", encoding="utf-8-sig") as f:
                for row in csv.DictReader(f):
                    send_row(row, "firms_archive", file.name)
                    sent += 1
            print(f"Processed {file.name} :{sent} rows")


def read_fires_live():
    map_key = os.environ["FIRMS_MAP_KEY"]
    for src in LIVE_SOURCES:
        url = FIRMS_URL.format(key=map_key, source=src, area=LIVE_AREA, days=LIVE_DAYS)
        while True:
            r = requests.get(url, timeout=120)
            if r.status_code == 429:
                print("rate limited, sleeping 60s")
                time.sleep(60)
                continue
            r.raise_for_status()
            break

        text = r.text.lstrip("\ufeff")
        if not text.lower().startswith("latitude"):
            print(f"{src}: unexpected response, skipped: {text[:200]}")
            continue

        request_id = f"{src}_last{LIVE_DAYS}d_{datetime.now(timezone.utc):%Y%m%dT%H%M}"
        sent = 0
        for row in csv.DictReader(io.StringIO(text)):
            send_row(row, "firms_api", request_id)
            sent += 1
        print(f"{src}: {sent} rows")
        time.sleep(1)


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "backfill"
    if mode not in ("backfill", "live"):
        sys.exit("usage: python producer.py [backfill|live]")

    if mode == "live":
        read_fires_live()
    else:
        read_fires_raw()

    producer.flush()
    print(f"Done ({mode}). Delivered: {stats['ok']}, failed: {stats['failed']}")