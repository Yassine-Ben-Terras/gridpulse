with source as (
    select * from {{ source('raw', 'entsoe_raw') }}
    where doc_kind = 'load'
),

with_ts as (
    -- Compute the real timestamp using this row's ACTUAL resolution rather
    -- than assuming 1-hour steps (ENTSO-E commonly reports at 15-min
    -- resolution).
    select
        zone,
        period_start::timestamptz
            + (position - 1) * (coalesce(resolution_minutes, 60) * interval '1 minute') as ts,
        quantity as load_mw,
        loaded_at
    from source
),

deduped as (
    -- Dedup at native (sub-hourly) resolution first: different fetches can
    -- describe the same real timestamp with different period_start/position
    -- pairs, so this must key on the computed ts, not the raw encoding.
    select
        *,
        row_number() over (
            partition by zone, ts
            order by loaded_at desc
        ) as rn
    from with_ts
),

hourly as (
    -- Aggregate up to hourly: the rest of this pipeline (weather join,
    -- forecasting lag features) operates on hourly granularity.
    select
        zone,
        date_trunc('hour', ts) as ts,
        avg(load_mw) as load_mw
    from deduped
    where rn = 1
    group by zone, date_trunc('hour', ts)
)

select
    zone || '_' || ts::text as row_key,
    zone,
    ts,
    load_mw
from hourly