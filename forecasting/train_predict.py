"""
Baseline day-ahead forecasting job.

Reads `marts.fct_demand_weather`, builds simple lagged/calendar features,
trains a gradient-boosted regressor per target (demand, wind, solar), and
writes predictions to `forecasts` with a timestamp and model version.

This is intentionally a baseline -- swap in a real feature store, backtesting,
and model registry as the project matures.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone

import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor
from sqlalchemy import create_engine, text

MODEL_VERSION = "baseline-gbr-v3"
TARGETS = {
    "load_mw": "demand",
    "wind_mw": "wind_generation",
    "solar_mw": "solar_generation",
}
# Below this many usable training rows, skip rather than fit a near-meaningless
# model. Kept low deliberately -- this is a baseline meant to produce *something*
# real early on, not a production-quality forecast yet (see roadmap: revisit
# once weeks of history have accumulated and add the 168h lag back in).
MIN_TRAIN_ROWS = 50


def _engine():
    url = (
        f"postgresql+psycopg2://{os.environ['WAREHOUSE_USER']}:{os.environ['WAREHOUSE_PASSWORD']}"
        f"@{os.environ['WAREHOUSE_HOST']}:{os.environ['WAREHOUSE_PORT']}/{os.environ['WAREHOUSE_DB']}"
    )
    return create_engine(url)


def _build_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["zone", "ts"]).copy()
    df["hour"] = df["ts"].dt.hour
    df["dayofweek"] = df["ts"].dt.dayofweek
    df["month"] = df["ts"].dt.month
    # Only a 24h lag for now -- a 168h (weekly) lag needs a full week of prior
    # history just to produce its first non-null value, which with only ~1-2
    # weeks of total history leaves almost no usable training rows. Add it
    # back in once there's genuinely a month+ of accumulated data.
    for col in ["load_mw", "wind_mw", "solar_mw"]:
        df[f"{col}_lag24"] = df.groupby("zone")[col].shift(24)
    return df


def _feature_columns() -> list[str]:
    return [
        "hour", "dayofweek", "month",
        "temperature_2m", "wind_speed_10m", "shortwave_radiation",
        "load_mw_lag24",
        "wind_mw_lag24",
        "solar_mw_lag24",
    ]


def _ensure_forecasts_table(engine):
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS forecasts (
                zone TEXT,
                ts TIMESTAMPTZ,
                target TEXT,
                predicted_value DOUBLE PRECISION,
                model_version TEXT,
                predicted_at TIMESTAMPTZ,
                PRIMARY KEY (zone, ts, target, model_version)
            )
        """))


def main() -> None:
    engine = _engine()
    _ensure_forecasts_table(engine)

    with engine.connect() as conn:
        result = conn.execute(text("select * from marts.fct_demand_weather"))
        df = pd.DataFrame(result.fetchall(), columns=list(result.keys()))
    if not df.empty:
        df["ts"] = pd.to_datetime(df["ts"], utc=True)
    if df.empty:
        print("[train_predict] no data in mart yet, skipping.")
        return

    df = _build_features(df)
    feature_cols = _feature_columns()

    predictions = []
    predicted_at = datetime.now(timezone.utc)

    for zone, zdf in df.groupby("zone"):
        for target_col, target_name in TARGETS.items():
            train_df = zdf.dropna(subset=feature_cols + [target_col])
            if len(train_df) < MIN_TRAIN_ROWS:
                print(f"[train_predict] not enough rows yet for {zone}/{target_name} "
                      f"({len(train_df)} < {MIN_TRAIN_ROWS}), skipping.")
                continue

            X = train_df[feature_cols]
            y = train_df[target_col]

            model = GradientBoostingRegressor(random_state=42)
            model.fit(X, y)

            # Predict on the most recent complete feature row as a simple next-step forecast.
            latest_row = zdf.dropna(subset=feature_cols).iloc[[-1]]
            if latest_row.empty:
                continue
            next_ts = latest_row["ts"].iloc[0] + pd.Timedelta(hours=1)

            # Calendar features (hour/dayofweek/month) must describe the hour
            # being predicted (next_ts), not the hour of latest_row -- solar
            # in particular has a strong diurnal cycle, so predicting for e.g.
            # 00:00 using hour=23's calendar features is a real mismatch even
            # though the underlying weather/lag values are reasonable proxies
            # for one hour ahead.
            pred_features = latest_row[feature_cols].copy()
            pred_features["hour"] = next_ts.hour
            pred_features["dayofweek"] = next_ts.dayofweek
            pred_features["month"] = next_ts.month

            pred_value = float(model.predict(pred_features)[0])

            predictions.append({
                "zone": zone,
                "ts": next_ts,
                "target": target_name,
                "predicted_value": pred_value,
                "model_version": MODEL_VERSION,
                "predicted_at": predicted_at,
            })

    if not predictions:
        print("[train_predict] no predictions generated.")
        return

    pred_df = pd.DataFrame(predictions)
    with engine.begin() as conn:
        for _, row in pred_df.iterrows():
            conn.execute(text("""
                INSERT INTO forecasts (zone, ts, target, predicted_value, model_version, predicted_at)
                VALUES (:zone, :ts, :target, :predicted_value, :model_version, :predicted_at)
                ON CONFLICT (zone, ts, target, model_version)
                DO UPDATE SET predicted_value = EXCLUDED.predicted_value, predicted_at = EXCLUDED.predicted_at
            """), row.to_dict())

    print(f"[train_predict] wrote {len(pred_df)} predictions.")


if __name__ == "__main__":
    main()