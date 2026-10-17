import os
import sys
import time
from datetime import datetime,timedelta,timezone
import requests
import yaml
from dotenv import load_dotenv
from pathlib import Path
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField, StringSerializer

load_dotenv()
ROOT = Path(__file__).resolve().parent.parent

TOPIC="aq.raw"
BASE_URL="https://api.openaq.org/v3"
HEADERS={"X-API-Key": os.environ["OPENAQ_API_KEY"]}
PAGE_LIMIT=1000
LIVE_LOOKBACK_DAYS=5

SEASONS=[("2023-09-15", "2023-12-15"), ("2024-09-15", "2024-12-15"), ("2025-09-15", "2025-12-15"),("2026-09-15T00:00:00Z", "2026-10-14T00:00:00Z")]

producer= Producer({"bootstrap.servers":os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),"enable.idempotence":True,"linger.ms":50})
schema_registry = SchemaRegistryClient({"url": os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")})
avro_serializer = AvroSerializer(schema_registry, (ROOT / "schemas" / "aq_raw.avsc").read_text(encoding="utf-8"))


stats={"ok":0,"failed":0}

def err_report(err,msg):
    if err is not None :
        stats["failed"]+=1
        print(f"There occurred this {err}")

    else:
        stats["ok"]+=1

def fetch_hours(sensor_id,dt_from,dt_to):
    page=1
    while True:
        r=requests.get(f"{BASE_URL}/sensors/{sensor_id}/hours",headers=HEADERS,params={"datetime_from":dt_from,"datetime_to":dt_to,"limit":PAGE_LIMIT,"page":page},timeout=30 )
        if r.status_code==429:
            print("rate limited")
            time.sleep(60)
            continue
        r.raise_for_status()
        results=r.json().get("results",[])
        yield from  results
        time.sleep(1.1)
        if len(results)<PAGE_LIMIT:
             break
        page+=1

if __name__=="__main__":
    mode=sys.argv[1] if len(sys.argv)>1 else "backfill"
    if mode not in ("backfill","live"):
        sys.exit("usage:python aq_producer.py [backfill|live]")
    with open(ROOT / "stations.yaml", encoding="utf-8") as f:
         stations=yaml.safe_load(f)["stations"]

    if mode == "live":
        now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        windows = [((now - timedelta(days=LIVE_LOOKBACK_DAYS)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    now.strftime("%Y-%m-%dT%H:%M:%SZ"))]
    else:
        windows = SEASONS

    for st in stations :
        available_from=f'{st["available_from"]}T00:00:00Z'

        for dt_from,dt_to in windows:
            if dt_to <=available_from:
                continue
            dt_from=max(dt_from,available_from)

            sent=0
            for res in fetch_hours(st["pm25_sensor_id"], dt_from, dt_to):
                parameter = res.get("parameter") or {}
                period = res.get("period") or {}
                coverage = res.get("coverage") or {}

                msg = {
                    "station_key": st["station_key"],
                    "location_id": st["location_id"],
                    "sensor_id": st["pm25_sensor_id"],
                    "parameter": parameter.get("name"),
                    "units": parameter.get("units"),
                    "value": res.get("value"),
                    "datetime_from_utc": (period.get("datetimeFrom") or {}).get("utc"),
                    "datetime_to_utc": (period.get("datetimeTo") or {}).get("utc"),
                    "observed_count": coverage.get("observedCount"),
                    "expected_count": coverage.get("expectedCount"),
                    "percent_coverage": coverage.get("percentCoverage"),
                    "has_flags": (res.get("flagInfo") or {}).get("hasFlags"),
                    "source": "openaq_v3",
                    "request_window": f"{dt_from}/{dt_to}",
                    "ingested_at": datetime.now(timezone.utc).isoformat(),
                }

                value = avro_serializer(msg,SerializationContext(TOPIC, MessageField.VALUE))
                while True:
                    try:
                        producer.produce(TOPIC, key=st["station_key"].encode("utf-8"), value=value, callback=err_report)
                        break
                    except BufferError:
                        producer.poll(1)
                producer.poll(0)
                sent += 1

            print(f'{st["station_key"]:<22} {dt_from[:10]} -> {dt_to[:10]}: {sent} rows')

    producer.flush()
    print(f"Done ({mode}). Delivered: {stats['ok']}, failed: {stats['failed']}")