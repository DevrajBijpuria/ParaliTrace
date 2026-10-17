"""
For every candidate sensor in stations.yaml, ask OpenAQ for that sensor's own first/last
timestamps, and count hourly rows in a 3-day test window during peak season (Nov 1-3, 2024).
Pick the sensor ID with full coverage and paste it into pm25_sensor_id.

Setup:  pip install requests python-dotenv pyyaml
Run:    python check_sensors.py
"""
import os
import time
import requests
import yaml
from dotenv import load_dotenv

load_dotenv()
HEADERS = {"X-API-Key": "210110b4ed2094c87ffdf83da56d4d47f4e6ba8b742979e2a2269e339a4570fb"}
BASE = "https://api.openaq.org/v3"

with open("stations.yaml", encoding="utf-8") as f:
    stations = yaml.safe_load(f)["stations"]

print(f'{"station":<22}{"sensor":>10}  {"first":<11}{"last":<11}{"rows Nov1-3 2024":>17}')
for st in stations:
    for sid in st["candidate_sensor_ids"]:
        meta = requests.get(f"{BASE}/sensors/{sid}", headers=HEADERS, timeout=30)
        meta.raise_for_status()
        s = meta.json()["results"][0]
        first = ((s.get("datetimeFirst") or {}).get("utc") or "")[:10]
        last = ((s.get("datetimeLast") or {}).get("utc") or "")[:10]
        time.sleep(1.1)

        hrs = requests.get(
            f"{BASE}/sensors/{sid}/hours",
            headers=HEADERS,
            params={"datetime_from": "2025-11-01T00:00:00Z", "datetime_to": "2025-11-04T00:00:00Z", "limit": 100},
            timeout=30,
        )
        rows = len(hrs.json().get("results", [])) if hrs.ok else f"HTTP {hrs.status_code}"
        time.sleep(1.1)

        print(f'{st["station_key"]:<22}{sid:>10}  {first:<11}{last:<11}{rows:>17}')