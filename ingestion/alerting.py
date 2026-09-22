"""
Shared Airflow on_failure_callback: logs task failures into the warehouse
itself (a `pipeline_alerts` table) rather than requiring SMTP or Slack
credentials. Query it directly, or check the FastAPI /metrics endpoint, to
know something broke without having to check the Airflow UI by hand.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone

from sqlalchemy import create_engine, text


def _warehouse_engine():
    url = (
        f"postgresql+psycopg2://{os.environ['WAREHOUSE_USER']}:{os.environ['WAREHOUSE_PASSWORD']}"
        f"@{os.environ['WAREHOUSE_HOST']}:{os.environ['WAREHOUSE_PORT']}/{os.environ['WAREHOUSE_DB']}"
    )
    return create_engine(url)


def _ensure_table(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS pipeline_alerts (
                id SERIAL PRIMARY KEY,
                dag_id TEXT,
                task_id TEXT,
                run_id TEXT,
                logical_date TIMESTAMPTZ,
                error_message TEXT,
                failed_at TIMESTAMPTZ
            )
        """))


def log_failure_to_warehouse(context: dict) -> None:
    """
    Attach as on_failure_callback in a DAG's default_args:
        default_args = {..., "on_failure_callback": log_failure_to_warehouse}

    Deliberately never raises -- a broken alerting path should never mask or
    interfere with the real task failure Airflow is already reporting.
    """
    try:
        dag = context.get("dag")
        dag_id = dag.dag_id if dag else None
        task_instance = context.get("task_instance")
        task_id = task_instance.task_id if task_instance else None
        run_id = context.get("run_id")
        logical_date = context.get("logical_date") or context.get("execution_date")
        exception = context.get("exception")
        error_message = str(exception) if exception else "Unknown failure (no exception in context)"

        engine = _warehouse_engine()
        _ensure_table(engine)
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO pipeline_alerts (dag_id, task_id, run_id, logical_date, error_message, failed_at)
                VALUES (:dag_id, :task_id, :run_id, :logical_date, :error_message, :failed_at)
            """), {
                "dag_id": dag_id,
                "task_id": task_id,
                "run_id": run_id,
                "logical_date": logical_date,
                "error_message": error_message[:2000],
                "failed_at": datetime.now(timezone.utc),
            })
    except Exception as e:
        print(f"[alerting] failed to log failure to warehouse: {e}")