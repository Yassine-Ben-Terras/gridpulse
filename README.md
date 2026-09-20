# Weather-Driven Electricity Demand & Renewables Forecasting Platform

Local, container-based platform: ENTSO-E + Open-Meteo → MinIO (bronze) →
Postgres/dbt (silver/gold) → forecasting → Metabase + a read-only FastAPI layer.

Covers three bidding zones out of the box: **France (FR)**, **Germany-Luxembourg (DE)**,
and **Spain (ES)**.

## 1. Prerequisites

- Docker + Docker Compose v2
- A free ENTSO-E API token: https://transparency.entsoe.eu (Account Settings → Web API Security Token)

## 2. Configure

```bash
cp .env.example .env
```

Then edit `.env`:
- `ENTSOE_API_TOKEN` — your real token
- `AIRFLOW__CORE__FERNET_KEY` — generate one:
  ```bash
  python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
  ```
- Change every `changeme*` password to something real, even for local dev.

## 3. Bring the stack up

```bash
docker compose up -d --build
```

First boot builds the custom Airflow image (with requests/pandas/scikit-learn/dbt-core/
dbt-postgres baked in) and the FastAPI image, runs `airflow-init` once to migrate the
metadata DB and create the admin user, then starts everything else.

## 4. Verify

| Service            | URL                            | Credentials                                       |
|--------------------|---------------------------------|----------------------------------------------------|
| Airflow UI         | http://localhost:8080          | `AIRFLOW_ADMIN_USER` / `..._PASSWORD` from `.env`  |
| MinIO console      | http://localhost:9001          | `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD`          |
| Warehouse Postgres | localhost:5432 (psql/DBeaver)  | `POSTGRES_USER` / `POSTGRES_PASSWORD`              |
| Metabase           | http://localhost:3000          | set up on first visit — connect to Postgres using host `postgres` (the Docker service name), not `localhost` |
| FastAPI            | http://localhost:8000/docs     | none (read-only, no auth yet — see Known Limitations) |

In Airflow, unpause the three DAGs:
- `ingest_entsoe` — hourly ENTSO-E pull (load, generation by type, day-ahead price) → MinIO, one task per zone
- `ingest_openmeteo` — hourly Open-Meteo pull (temperature, wind, solar radiation) → MinIO, one task per zone
- `run_dbt_and_forecast` — waits for both, loads raw MinIO data into
  `raw.*` Postgres tables (idempotent — tracks which MinIO objects are already
  loaded), runs `dbt run`/`dbt test`/`dbt source freshness`, then trains/predicts

Trigger each DAG manually once instead of waiting for the top of the hour if you
want to see data flow immediately.

## 5. Repository layout

```
project/
├── docker-compose.yml
├── .env.example
├── airflow/
│   ├── Dockerfile            # base Airflow image + requirements.txt
│   ├── requirements.txt
│   └── dags/
│       ├── ingest_entsoe.py           # one task per zone (FR/DE/ES)
│       ├── ingest_openmeteo.py        # one task per zone (FR/DE/ES)
│       └── run_dbt_and_forecast.py
├── ingestion/
│   ├── state.py               # per-source/zone "last ingested" marker + "already loaded object" tracker in MinIO
│   ├── alerting.py            # Airflow on_failure_callback -> logs into `pipeline_alerts` table (no SMTP/Slack needed)
│   ├── entsoe_fetcher.py
│   ├── openmeteo_fetcher.py
│   └── load_to_warehouse.py   # bronze (MinIO) -> raw.* Postgres tables; resolution-aware (handles ENTSO-E's 15-min data), idempotent
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── macros/
│   │   └── generate_schema_name.sql   # makes custom schemas (staging/marts) literal, not prefixed
│   └── models/
│       ├── staging/            # sources.yml (with freshness checks) + stg_* views, deduped + hourly-aggregated
│       └── marts/               # fct_demand_weather.sql, with unique/not_null tests on a row_key surrogate
├── forecasting/
│   └── train_predict.py        # baseline GradientBoostingRegressor per zone/target, writes to `forecasts`
├── api/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── main.py                 # FastAPI: /health, /forecasts, /metrics (includes recent pipeline_alerts)
└── ... (Postgres/MinIO/Metabase data lives in named Docker volumes, not host folders)
```

Named volumes used: `airflow_pg_data`, `pg_data`, `minio_data`, `metabase_data`. List them
with `docker volume ls`, wipe one with `docker volume rm <name>` if you need a clean slate
(container must be stopped first).

## 6. Data quality & observability

- **Idempotent everywhere**: ingestion tracks "last ingested timestamp" per zone;
  the warehouse loader tracks "already loaded MinIO objects"; dbt staging models
  dedup on a computed timestamp (not the raw source encoding) before aggregating
  15-minute ENTSO-E data up to hourly.
- **dbt tests**: `unique`/`not_null` on every staging model and the mart (via a
  `row_key` surrogate column), plus source freshness checks (`dbt source
  freshness`, informational — logged but non-blocking).
- **Failure alerting**: any Airflow task failure across all three DAGs gets
  logged into a `pipeline_alerts` table in the warehouse (see
  `ingestion/alerting.py`) — no SMTP/Slack credentials required. Surfaced via
  `GET /metrics` on the FastAPI service.

