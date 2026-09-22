import sys
from datetime import datetime, timedelta

sys.path.insert(0, "/opt/airflow/ingestion")

from airflow import DAG
from airflow.operators.python import PythonOperator

from openmeteo_fetcher import run as run_openmeteo
from alerting import log_failure_to_warehouse

# Coordinates should match the bidding zones tracked in ingest_entsoe.py so
# weather and energy data join cleanly downstream in dbt.
ZONES = [
    {"lat": 48.8566, "lon": 2.3522, "zone_label": "FR"},    # Paris
    {"lat": 51.1657, "lon": 10.4515, "zone_label": "DE"},   # geographic center of Germany
    {"lat": 40.4168, "lon": -3.7038, "zone_label": "ES"},   # Madrid
]

default_args = {
    "owner": "energy-weather-platform",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": log_failure_to_warehouse,
}

with DAG(
    dag_id="ingest_openmeteo",
    description="Hourly incremental pull of Open-Meteo weather data into MinIO (bronze)",
    default_args=default_args,
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ingestion", "bronze", "openmeteo"],
) as dag:
    for zone in ZONES:
        PythonOperator(
            task_id=f"fetch_openmeteo_{zone['zone_label']}",
            python_callable=run_openmeteo,
            op_kwargs=zone,
        )