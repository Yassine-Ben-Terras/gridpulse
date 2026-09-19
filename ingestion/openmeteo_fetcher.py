"""
Pulls hourly temperature, wind speed, and solar radiation from the free
Open-Meteo API and lands raw JSON in MinIO, partitioned by source/zone/date.

Docs: https://open-meteo.com/en/docs

Run standalone for local testing:
    python openmeteo_fetcher.py --lat 48.85 --lon 2.35 --zone-label FR

Called from Airflow via airflow/dags/ingest_openmeteo.py.
"""
from __future__ import annotations

import argparse
import io
import json
import os
from datetime import datetime, timedelta, timezone

import requests

from state import get_client, get_last_ingested, set_last_ingested

OPENMETEO_BASE_URL = "https://api.open-meteo.com/v1/forecast"
SOURCE_NAME = "openmeteo"
HOURLY_VARS = "temperature_2m,wind_speed_10m,shortwave_radiation"


def fetch_window(lat: float, lon: float, start_date: str, end_date: str) -> dict:
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": HOURLY_VARS,
        "start_date": start_date,
        "end_date": end_date,
        "timezone": "UTC",
    }
    resp = requests.get(OPENMETEO_BASE_URL, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def run(lat: float, lon: float, zone_label: str) -> None:
    bucket = os.environ["MINIO_BUCKET"]
    client = get_client(
        os.environ["MINIO_ENDPOINT"],
        os.environ["MINIO_ROOT_USER"],
        os.environ["MINIO_ROOT_PASSWORD"],
    )

    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    last = get_last_ingested(client, bucket, SOURCE_NAME, zone_label)
    period_start = (last or now - timedelta(days=2))

    if period_start.date() > now.date():
        print(f"[{SOURCE_NAME}/{zone_label}] already up to date.")
        return

    payload = fetch_window(lat, lon, period_start.strftime("%Y-%m-%d"), now.strftime("%Y-%m-%d"))
    content = json.dumps(payload).encode()

    date_str = period_start.strftime("%Y-%m-%d")
    object_name = f"{SOURCE_NAME}/{zone_label}/{date_str}/weather_{period_start:%Y%m%d}_{now:%Y%m%d}.json"
    client.put_object(
        bucket, object_name,
        data=io.BytesIO(content), length=len(content),
        content_type="application/json",
    )
    print(f"[{SOURCE_NAME}/{zone_label}] wrote {object_name}")

    set_last_ingested(client, bucket, SOURCE_NAME, zone_label, now)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--lat", type=float, default=48.8566, help="Latitude (default: Paris)")
    parser.add_argument("--lon", type=float, default=2.3522, help="Longitude (default: Paris)")
    parser.add_argument("--zone-label", default="FR", help="Short label used in the storage path")
    args = parser.parse_args()
    run(args.lat, args.lon, args.zone_label)
