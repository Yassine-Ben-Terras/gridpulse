"""
Lightweight read-only API over the warehouse, so other software can consume
forecasts and pipeline health without going through Metabase or querying
Postgres directly.

Endpoints:
    GET /health              -- liveness check
    GET /forecasts           -- latest prediction per (zone, target), filterable
    GET /metrics             -- row counts and freshness per zone, for monitoring
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, ConfigDict
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

app = FastAPI(
    title="Weather-Energy Platform API",
    description="Read-only API over the warehouse: forecasts and pipeline health metrics.",
    version="1.0.0",
)

_engine: Optional[Engine] = None


def get_engine() -> Engine:
    global _engine
    if _engine is None:
        url = (
            f"postgresql+psycopg2://{os.environ['WAREHOUSE_USER']}:{os.environ['WAREHOUSE_PASSWORD']}"
            f"@{os.environ['WAREHOUSE_HOST']}:{os.environ['WAREHOUSE_PORT']}/{os.environ['WAREHOUSE_DB']}"
        )
        _engine = create_engine(url, pool_pre_ping=True)
    return _engine


class ForecastOut(BaseModel):
    # "model_version" collides with Pydantic v2's reserved "model_" namespace;
    # this keeps the field name matching the actual DB column instead of
    # renaming it, and just silences the resulting (harmless) warning.
    model_config = ConfigDict(protected_namespaces=())

    zone: str
    ts: datetime
    target: str
    predicted_value: float
    model_version: str
    predicted_at: datetime


class ZoneMetrics(BaseModel):
    zone: str
    row_count: int
    earliest_ts: Optional[datetime]
    latest_ts: Optional[datetime]
    latest_forecast_at: Optional[datetime]


class AlertOut(BaseModel):
    dag_id: Optional[str]
    task_id: Optional[str]
    run_id: Optional[str]
    logical_date: Optional[datetime]
    error_message: Optional[str]
    failed_at: datetime


class MetricsOut(BaseModel):
    generated_at: datetime
    zones: list[ZoneMetrics]
    recent_alerts: list[AlertOut]


@app.get("/health")
def health():
    try:
        with get_engine().connect() as conn:
            conn.execute(text("select 1"))
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"database unreachable: {e}")


@app.get("/forecasts", response_model=list[ForecastOut])
def get_forecasts(
    zone: Optional[str] = Query(None, description="Filter to one zone, e.g. FR"),
    target: Optional[str] = Query(
        None, description="Filter to one target: demand, wind_generation, or solar_generation"
    ),
):
    """
    Returns the freshest prediction per (zone, target) -- i.e. the row with
    the latest predicted_at -- regardless of which model_version produced it.
    Multiple model versions can coexist in the table (e.g. across retrains);
    this always surfaces the most recently computed one, not necessarily the
    row with the "highest" version string.
    """
    conditions = []
    params = {}
    if zone:
        conditions.append("zone = :zone")
        params["zone"] = zone
    if target:
        conditions.append("target = :target")
        params["target"] = target
    where_clause = f"where {' and '.join(conditions)}" if conditions else ""

    query = text(f"""
        select distinct on (zone, target)
            zone, ts, target, predicted_value, model_version, predicted_at
        from forecasts
        {where_clause}
        order by zone, target, predicted_at desc
    """)

    with get_engine().connect() as conn:
        rows = conn.execute(query, params).mappings().all()

    if not rows and (zone or target):
        raise HTTPException(status_code=404, detail="No forecasts match that filter.")

    return [dict(row) for row in rows]


@app.get("/metrics", response_model=MetricsOut)
def get_metrics():
    """
    Per-zone row counts and freshness from the mart, each zone's most recent
    forecast timestamp, and the most recent pipeline failures (if any) --
    logged by the Airflow on_failure_callback in ingestion/alerting.py. A
    quick way for another service (or a status page) to check the pipeline
    is actually current and healthy without querying Postgres directly.
    """
    mart_query = text("""
        select zone, count(*) as row_count, min(ts) as earliest_ts, max(ts) as latest_ts
        from marts.fct_demand_weather
        group by zone
        order by zone
    """)
    forecast_query = text("""
        select zone, max(predicted_at) as latest_forecast_at
        from forecasts
        group by zone
    """)
    alerts_query = text("""
        select dag_id, task_id, run_id, logical_date, error_message, failed_at
        from pipeline_alerts
        order by failed_at desc
        limit 10
    """)

    with get_engine().connect() as conn:
        mart_rows = {row["zone"]: dict(row) for row in conn.execute(mart_query).mappings().all()}
        forecast_rows = {row["zone"]: row["latest_forecast_at"] for row in conn.execute(forecast_query).mappings().all()}
        try:
            alert_rows = [dict(row) for row in conn.execute(alerts_query).mappings().all()]
        except Exception:
            # pipeline_alerts is created lazily on the first real failure --
            # no failures yet simply means the table doesn't exist.
            alert_rows = []

    zones = [
        ZoneMetrics(
            zone=zone,
            row_count=data["row_count"],
            earliest_ts=data["earliest_ts"],
            latest_ts=data["latest_ts"],
            latest_forecast_at=forecast_rows.get(zone),
        )
        for zone, data in mart_rows.items()
    ]

    return MetricsOut(
        generated_at=datetime.now(timezone.utc),
        zones=zones,
        recent_alerts=[AlertOut(**row) for row in alert_rows],
    )