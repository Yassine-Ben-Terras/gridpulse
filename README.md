# Weather-Driven Electricity Demand & Renewables Forecasting Platform

Local, container-based platform: ENTSO-E + Open-Meteo → MinIO (bronze) →
Postgres/dbt (silver/gold) → forecasting → Metabase.

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

First boot builds the custom Airflow image (with requests/pandas/scikit-learn/dbt-postgres
baked in), runs `airflow-init` once to migrate the metadata DB and create the admin user,
then starts everything else.

## 4. Verify

| Service         | URL                          | Credentials                          |
|------------------|-------------------------------|---------------------------------------|
| Airflow UI       | http://localhost:8080         | `AIRFLOW_ADMIN_USER` / `..._PASSWORD` from `.env` |
| MinIO console    | http://localhost:9001         | `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` |
| Warehouse Postgres | localhost:5432 (psql/DBeaver) | `POSTGRES_USER` / `POSTGRES_PASSWORD` |
| Metabase         | http://localhost:3000         | set up on first visit                 |

In Airflow, unpause the three DAGs:
- `ingest_entsoe` — hourly ENTSO-E pull → MinIO
- `ingest_openmeteo` — hourly Open-Meteo pull → MinIO
- `run_dbt_and_forecast` — waits for both, loads raw MinIO data into
  `raw.*` Postgres tables, runs `dbt run`/`dbt test`, then trains/predicts

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
│       ├── ingest_entsoe.py
│       ├── ingest_openmeteo.py
│       └── run_dbt_and_forecast.py
├── ingestion/
│   ├── state.py               # per-source/zone "last ingested" marker in MinIO
│   ├── entsoe_fetcher.py
│   ├── openmeteo_fetcher.py
│   └── load_to_warehouse.py   # bronze (MinIO) -> raw.* Postgres tables
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/            # sources.yml + stg_* views, one per raw source
│       └── marts/               # fct_demand_weather.sql
├── forecasting/
│   └── train_predict.py        # baseline GradientBoostingRegressor, writes to `forecasts`
├── minio-data/                 # gitignored volume
├── pg-data/                    # gitignored volume
└── airflow-pg-data/            # gitignored volume (Airflow's own metadata DB)
```
