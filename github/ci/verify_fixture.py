"""
Runs against the CI's ephemeral Postgres after `dbt run` on the seed fixture
(.github/ci/seed_fixture.sql). Checks the mart's computed values exactly
match what the fixture was designed to produce, so CI actually validates the
pipeline's logic -- dedup, 15-min-to-hourly aggregation, NULL-generation
coalesced to zero -- not merely that dbt exits with code 0.
"""
import os
import sys

from sqlalchemy import create_engine, text

EXPECTED = {
    "2026-01-01 00:00:00+00": {
        "load_mw": 1265.0,
        "wind_mw": 315.0,
        "solar_mw": 0.0,
        "day_ahead_price": 53.0,
        "temperature_2m": 5.0,
    },
    "2026-01-01 01:00:00+00": {
        "load_mw": 1115.0,
        "wind_mw": 355.0,
        "solar_mw": 500.0,
        "day_ahead_price": 63.0,
        "temperature_2m": 6.0,
    },
}


def main() -> int:
    url = (
        f"postgresql+psycopg2://{os.environ['WAREHOUSE_USER']}:{os.environ['WAREHOUSE_PASSWORD']}"
        f"@{os.environ['WAREHOUSE_HOST']}:{os.environ['WAREHOUSE_PORT']}/{os.environ['WAREHOUSE_DB']}"
    )
    engine = create_engine(url)

    failures = []
    with engine.connect() as conn:
        rows = conn.execute(text("""
            select ts::text as ts, load_mw, wind_mw, solar_mw, day_ahead_price, temperature_2m
            from marts.fct_demand_weather
            where zone = 'FR'
            order by ts
        """)).mappings().all()

    actual_by_ts = {row["ts"]: dict(row) for row in rows}

    if set(actual_by_ts.keys()) != set(EXPECTED.keys()):
        failures.append(
            f"Expected rows for ts {sorted(EXPECTED.keys())}, got {sorted(actual_by_ts.keys())}"
        )

    for ts, expected_values in EXPECTED.items():
        actual = actual_by_ts.get(ts)
        if actual is None:
            continue  # already reported above
        for col, expected_value in expected_values.items():
            actual_value = actual.get(col)
            if actual_value is None or abs(float(actual_value) - expected_value) > 0.01:
                failures.append(
                    f"[{ts}] {col}: expected {expected_value}, got {actual_value}"
                )

    if failures:
        print("FIXTURE VERIFICATION FAILED:")
        for f in failures:
            print(f"  - {f}")
        return 1

    print("Fixture verification passed: dedup, hourly aggregation, and "
          "NULL-generation coalescing all produced the expected values.")
    return 0


if __name__ == "__main__":
    sys.exit(main())