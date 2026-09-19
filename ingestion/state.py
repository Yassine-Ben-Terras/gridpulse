"""
Tiny helper for tracking "last successfully ingested timestamp" per (source, zone)
so fetchers can pull only new hours and be safely re-run.

Implementation detail: stores a one-line marker object per source/zone as a small
JSON file in MinIO, at `_state/{source}/{zone}.json`. Swap this for a Postgres
state table later if you outgrow it.
"""
from __future__ import annotations

import io
import json
from datetime import datetime, timezone

from minio import Minio
from minio.error import S3Error


def get_client(endpoint: str, access_key: str, secret_key: str) -> Minio:
    return Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=False)


def _state_key(source: str, zone: str) -> str:
    return f"_state/{source}/{zone}.json"


def get_last_ingested(client: Minio, bucket: str, source: str, zone: str) -> datetime | None:
    try:
        resp = client.get_object(bucket, _state_key(source, zone))
        data = json.loads(resp.read())
        resp.close()
        resp.release_conn()
        return datetime.fromisoformat(data["last_ingested_at"])
    except S3Error as e:
        if e.code == "NoSuchKey":
            return None
        raise


def set_last_ingested(client: Minio, bucket: str, source: str, zone: str, ts: datetime) -> None:
    payload = json.dumps({"last_ingested_at": ts.astimezone(timezone.utc).isoformat()}).encode()
    client.put_object(
        bucket,
        _state_key(source, zone),
        data=io.BytesIO(payload),
        length=len(payload),
        content_type="application/json",
    )


def _loaded_objects_key(namespace: str) -> str:
    return f"_loaded/{namespace}.json"


def get_loaded_objects(client: Minio, bucket: str, namespace: str) -> set[str]:
    """
    Returns the set of MinIO object names already loaded into the warehouse
    for this namespace (e.g. "entsoe" or "openmeteo"), so a loader can skip
    re-inserting the same rows on every run.
    """
    try:
        resp = client.get_object(bucket, _loaded_objects_key(namespace))
        data = json.loads(resp.read())
        resp.close()
        resp.release_conn()
        return set(data.get("objects", []))
    except S3Error as e:
        if e.code == "NoSuchKey":
            return set()
        raise


def add_loaded_objects(client: Minio, bucket: str, namespace: str, new_objects: set[str]) -> None:
    """
    Merges new_objects into the existing loaded-objects set and persists it.
    Not safe for concurrent writers -- fine for a single hourly Airflow task.
    """
    existing = get_loaded_objects(client, bucket, namespace)
    merged = existing | new_objects
    payload = json.dumps({"objects": sorted(merged)}).encode()
    client.put_object(
        bucket,
        _loaded_objects_key(namespace),
        data=io.BytesIO(payload),
        length=len(payload),
        content_type="application/json",
    )