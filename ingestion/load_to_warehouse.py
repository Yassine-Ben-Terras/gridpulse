"""
Bridges the Bronze layer (raw files in MinIO) to the warehouse's `raw` schema
in Postgres, so dbt staging models have real source tables to select from.

Inserts use plain SQLAlchemy Core (parameterized INSERTs), not pandas.to_sql --
some pandas/SQLAlchemy version combinations fail to recognize a live Connection
as a valid "connectable" and fall back to a legacy raw-DBAPI code path that
doesn't exist on that object. Core INSERTs sidestep that detection entirely.

Idempotent: tracks which MinIO objects have already been loaded (via
ingestion/state.py's get_loaded_objects/add_loaded_objects, itself stored as a
small JSON marker in MinIO) so re-running this doesn't re-insert the same rows.
Not a general-purpose ETL framework -- swap it out if ingestion volume grows.
"""
from __future__ import annotations

import io
import json
import os
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import create_engine, text

from state import get_client, get_loaded_objects, add_loaded_objects


# Matches the fixed timestamp suffix entsoe_fetcher.py appends to every
# filename (e.g. "..._20260912T1900_20260914T1800.xml"), so whatever's left
# after stripping it is the true doc_kind -- unlike a naive split("_")[0],
# this correctly keeps multi-word kinds like "day_ahead_price" intact.
_DOC_KIND_SUFFIX_RE = re.compile(r"_\d{8}T\d{4}_\d{8}T\d{4}\.xml$")

# ENTSO-E resolution codes seen in practice: PT15M, PT30M, PT60M (sometimes
# written PT1H). Falls back to 60 minutes for anything unrecognized rather
# than crashing, though that fallback should never actually be hit in
# normal operation.
_RESOLUTION_RE = re.compile(r"PT(\d+)M")


def _resolution_to_minutes(resolution: str | None) -> int:
    if resolution:
        match = _RESOLUTION_RE.match(resolution)
        if match:
            return int(match.group(1))
        if resolution == "PT1H":
            return 60
    return 60


def _warehouse_engine():
    url = (
        f"postgresql+psycopg2://{os.environ['WAREHOUSE_USER']}:{os.environ['WAREHOUSE_PASSWORD']}"
        f"@{os.environ['WAREHOUSE_HOST']}:{os.environ['WAREHOUSE_PORT']}/{os.environ['WAREHOUSE_DB']}"
    )
    return create_engine(url)


def _ensure_tables(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS raw.entsoe_raw (
                zone TEXT,
                period_start TIMESTAMPTZ,
                position INTEGER,
                resolution_minutes INTEGER,
                psr_type TEXT,
                quantity DOUBLE PRECISION,
                price DOUBLE PRECISION,
                doc_kind TEXT,
                loaded_at TIMESTAMPTZ
            )
        """))
        # ALTER for anyone re-running against a table created before this
        # column existed -- harmless no-op if it's already there.
        conn.execute(text("""
            ALTER TABLE raw.entsoe_raw ADD COLUMN IF NOT EXISTS resolution_minutes INTEGER
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS raw.openmeteo_raw (
                zone TEXT,
                time TIMESTAMPTZ,
                temperature_2m DOUBLE PRECISION,
                wind_speed_10m DOUBLE PRECISION,
                shortwave_radiation DOUBLE PRECISION,
                loaded_at TIMESTAMPTZ
            )
        """))


def _insert_rows(engine, table: str, df: pd.DataFrame) -> None:
    if df.empty:
        return
    # Swap pandas NaN for real None so psycopg2 doesn't choke binding NaN
    # into non-float columns (e.g. psr_type, doc_kind).
    df = df.where(pd.notnull(df), None)
    records = df.to_dict(orient="records")
    columns = list(records[0].keys())
    col_list = ", ".join(columns)
    placeholders = ", ".join(f":{c}" for c in columns)
    stmt = text(f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})")
    with engine.begin() as conn:
        conn.execute(stmt, records)


def _parse_entsoe_xml(content: bytes, doc_kind: str, zone_label: str) -> pd.DataFrame:
    """
    Minimal ENTSO-E GL_MarketDocument parser: flattens <Point> elements
    (position/quantity, or position/price.amount) into rows. Extend as needed
    for multi-TimeSeries documents (e.g. generation split by psr type).

    Each Period carries its own <resolution> (commonly PT15M for generation,
    sometimes PT60M elsewhere) -- position is an index within that Period at
    that resolution, NOT a fixed 1-hour step. resolution_minutes is captured
    per row so downstream ts computation (in dbt) can convert position to a
    real timestamp correctly instead of assuming hourly steps universally.
    """
    root = ET.fromstring(content)
    rows = []
    for ts in root.iter():
        if ts.tag.endswith("TimeSeries"):
            psr_type = None
            for psr in ts.iter():
                if psr.tag.endswith("psrType"):
                    psr_type = psr.text
            for period in ts.iter():
                if period.tag.endswith("Period"):
                    start = None
                    resolution = None
                    for s in period.iter():
                        if s.tag.endswith("start"):
                            start = s.text
                        elif s.tag.endswith("resolution"):
                            resolution = s.text
                    resolution_minutes = _resolution_to_minutes(resolution)
                    for point in period.iter():
                        if point.tag.endswith("Point"):
                            position = quantity = price = None
                            for child in point:
                                tag = child.tag.split("}")[-1]
                                if tag == "position":
                                    position = int(child.text)
                                elif tag == "quantity":
                                    quantity = float(child.text)
                                elif tag == "price.amount":
                                    price = float(child.text)
                            rows.append({
                                "zone": zone_label,
                                "period_start": start,
                                "position": position,
                                "resolution_minutes": resolution_minutes,
                                "psr_type": psr_type,
                                "quantity": quantity,
                                "price": price,
                            })
    df = pd.DataFrame(rows)
    df["doc_kind"] = doc_kind
    df["loaded_at"] = datetime.now(timezone.utc)
    return df


def _parse_openmeteo_json(content: bytes, zone_label: str) -> pd.DataFrame:
    payload = json.loads(content)
    hourly = payload.get("hourly", {})
    df = pd.DataFrame(hourly)
    df["zone"] = zone_label
    df["loaded_at"] = datetime.now(timezone.utc)
    return df


def run() -> None:
    bucket = os.environ["MINIO_BUCKET"]
    client = get_client(
        os.environ["MINIO_ENDPOINT"],
        os.environ["MINIO_ROOT_USER"],
        os.environ["MINIO_ROOT_PASSWORD"],
    )
    engine = _warehouse_engine()
    _ensure_tables(engine)

    loaded_entsoe = get_loaded_objects(client, bucket, "entsoe")
    newly_loaded_entsoe = set()
    for obj in client.list_objects(bucket, prefix="entsoe/", recursive=True):
        if not obj.object_name.endswith(".xml") or obj.object_name in loaded_entsoe:
            continue
        zone_label = obj.object_name.split("/")[1]
        doc_kind = _DOC_KIND_SUFFIX_RE.sub("", obj.object_name.split("/")[-1])
        content = client.get_object(bucket, obj.object_name).read()
        df = _parse_entsoe_xml(content, doc_kind, zone_label)
        _insert_rows(engine, "raw.entsoe_raw", df)
        newly_loaded_entsoe.add(obj.object_name)
    if newly_loaded_entsoe:
        add_loaded_objects(client, bucket, "entsoe", newly_loaded_entsoe)
    print(f"[load_to_warehouse] loaded {len(newly_loaded_entsoe)} new entsoe objects "
          f"({len(loaded_entsoe)} already loaded previously).")

    loaded_openmeteo = get_loaded_objects(client, bucket, "openmeteo")
    newly_loaded_openmeteo = set()
    for obj in client.list_objects(bucket, prefix="openmeteo/", recursive=True):
        if not obj.object_name.endswith(".json") or obj.object_name in loaded_openmeteo:
            continue
        zone_label = obj.object_name.split("/")[1]
        content = client.get_object(bucket, obj.object_name).read()
        df = _parse_openmeteo_json(content, zone_label)
        _insert_rows(engine, "raw.openmeteo_raw", df)
        newly_loaded_openmeteo.add(obj.object_name)
    if newly_loaded_openmeteo:
        add_loaded_objects(client, bucket, "openmeteo", newly_loaded_openmeteo)
    print(f"[load_to_warehouse] loaded {len(newly_loaded_openmeteo)} new openmeteo objects "
          f"({len(loaded_openmeteo)} already loaded previously).")

    print("[load_to_warehouse] done.")


if __name__ == "__main__":
    run()