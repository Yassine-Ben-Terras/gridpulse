import sys
from datetime import datetime, timedelta

sys.path.insert(0, "/opt/airflow/forecasting")
sys.path.insert(0, "/opt/airflow/ingestion")

from airflow import DAG
from airflow.models import DagRun
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.python import PythonSensor

default_args = {
    "owner": "energy-weather-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _latest_run_succeeded(dag_id: str):
    """
    Checks the most recent DagRun of `dag_id`, regardless of execution_date.
    Deliberately simpler than ExternalTaskSensor: that sensor requires the two
    DAGs' execution_dates to line up exactly, which breaks the moment either
    DAG is triggered manually rather than by its own schedule.
    """
    def _poke(**context) -> bool:
        runs = DagRun.find(dag_id=dag_id)
        if not runs:
            return False
        latest = max(runs, key=lambda r: r.execution_date)
        return latest.state == "success"
    return _poke


with DAG(
    dag_id="run_dbt_and_forecast",
    description="Waits for both ingestion DAGs, then runs dbt transforms and the forecasting job",
    default_args=default_args,
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["transform", "silver", "gold", "forecasting"],
) as dag:

    wait_for_entsoe = PythonSensor(
        task_id="wait_for_entsoe",
        python_callable=_latest_run_succeeded("ingest_entsoe"),
        mode="reschedule",
        poke_interval=30,
        timeout=60 * 30,
    )

    wait_for_openmeteo = PythonSensor(
        task_id="wait_for_openmeteo",
        python_callable=_latest_run_succeeded("ingest_openmeteo"),
        mode="reschedule",
        poke_interval=30,
        timeout=60 * 30,
    )

    def _load_to_warehouse():
        from load_to_warehouse import run as run_loader
        run_loader()

    load_raw = PythonOperator(
        task_id="load_raw_to_warehouse",
        python_callable=_load_to_warehouse,
    )

    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        # Informational only: freshness warnings/errors are printed to the log
        # but never fail this task, so a stale source doesn't block the pipeline
        # the way a real test failure should. Check the logs periodically.
        bash_command="cd /opt/dbt && dbt source freshness --profiles-dir /opt/dbt || true",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/dbt && dbt run --profiles-dir /opt/dbt",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/dbt && dbt test --profiles-dir /opt/dbt",
    )

    def _run_forecast():
        from train_predict import main as run_forecast
        run_forecast()

    forecast = PythonOperator(
        task_id="run_forecast",
        python_callable=_run_forecast,
    )

    [wait_for_entsoe, wait_for_openmeteo] >> load_raw >> dbt_source_freshness >> dbt_run >> dbt_test >> forecast
