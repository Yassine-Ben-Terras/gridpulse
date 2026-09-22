import sys
from datetime import datetime, timedelta

sys.path.insert(0, "/opt/airflow/ingestion")

from airflow import DAG
from airflow.operators.python import PythonOperator

from entsoe_fetcher import run as run_entsoe
from alerting import log_failure_to_warehouse

# One task per bidding zone you want to track. Add more tuples as you expand coverage.
ZONES = [
    {"zone_eic": "10YFR-RTE------C", "zone_label": "FR"},
    {"zone_eic": "10Y1001A1001A82H", "zone_label": "DE"},  # Germany-Luxembourg bidding zone
    {"zone_eic": "10YES-REE------0", "zone_label": "ES"},
]

default_args = {
    "owner": "energy-weather-platform",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": log_failure_to_warehouse,
}

with DAG(
    dag_id="ingest_entsoe",
    description="Hourly incremental pull of ENTSO-E load/generation/price data into MinIO (bronze)",
    default_args=default_args,
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ingestion", "bronze", "entsoe"],
) as dag:
    for zone in ZONES:
        PythonOperator(
            task_id=f"fetch_entsoe_{zone['zone_label']}",
            python_callable=run_entsoe,
            op_kwargs=zone,
        )