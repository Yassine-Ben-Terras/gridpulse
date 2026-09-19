"""
called from Airflow via airflow/dags/ingest_entsoe.py.
"""
from __future__ import annotations

import argparse
import io
import os
from datetime import datetime, timedelta, timezone

import requests

from state import get_client, get_last_ingested, set_last_ingested

ENTSOE_BASE_URL = "https://web-api.tp.entsoe.eu/api"
SOURCE_NAME = "entsoe"

DOC_TYPES = {
    "load": {
        "documentType": "A65",
        "processType": "A16",  # realised
        "domain_params": lambda zone: {"outBiddingZone_Domain": zone},
    },
    "generation": {
        "documentType": "A75",
        "processType": "A16",  # realised
        "domain_params": lambda zone: {"in_Domain": zone},
    },
    "day_ahead_price": {
        "documentType": "A44",
        "domain_params": lambda zone: {"in_Domain": zone, "out_Domain": zone},
    },
}


def fetch_window(token: str, doc_config: dict, zone_eic: str, period_start: datetime, period_end: datetime) -> bytes:
    params = {
        "securityToken": token,
        "documentType": doc_config["documentType"],
        "periodStart": period_start.strftime("%Y%m%d%H%M"),
        "periodEnd": period_end.strftime("%Y%m%d%H%M"),
        **doc_config["domain_params"](zone_eic),
    }
    if "processType" in doc_config:
        params["processType"] = doc_config["processType"]
    resp = requests.get(ENTSOE_BASE_URL, params=params, timeout=30)
    resp.raise_for_status()
    return resp.content  # raw XML from ENTSO-E


def run(zone_eic: str, zone_label: str) -> None:
    token = os.environ["ENTSOE_API_TOKEN"]
    bucket = os.environ["MINIO_BUCKET"]
    client = get_client(
        os.environ["MINIO_ENDPOINT"],
        os.environ["MINIO_ROOT_USER"],
        os.environ["MINIO_ROOT_PASSWORD"],
    )

    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    last = get_last_ingested(client, bucket, SOURCE_NAME, zone_label)
    period_start = (last or now - timedelta(days=2))
    period_end = now

    if period_start >= period_end:
        print(f"[{SOURCE_NAME}/{zone_label}] already up to date.")
        return

    for name, doc_config in DOC_TYPES.items():
        content = fetch_window(token, doc_config, zone_eic, period_start, period_end)
        date_str = period_start.strftime("%Y-%m-%d")
        object_name = f"{SOURCE_NAME}/{zone_label}/{date_str}/{name}_{period_start:%Y%m%dT%H%M}_{period_end:%Y%m%dT%H%M}.xml"
        client.put_object(
            bucket, object_name,
            data=io.BytesIO(content), length=len(content),
            content_type="application/xml",
        )
        print(f"[{SOURCE_NAME}/{zone_label}] wrote {object_name}")

    set_last_ingested(client, bucket, SOURCE_NAME, zone_label, period_end)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--zone-eic", default="10YFR-RTE------C", help="ENTSO-E EIC bidding zone code")
    parser.add_argument("--zone-label", default="FR", help="Short label used in the storage path")
    args = parser.parse_args()
    run(args.zone_eic, args.zone_label)
