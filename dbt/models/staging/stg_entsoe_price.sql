with source as (
    select * from {{ source('raw', 'entsoe_raw') }}
    where doc_kind = 'day_ahead_price'
),

with_ts as (
    -- Compute the real timestamp using this row's ACTUAL resolution rather
    -- than assuming 1-hour steps -- see stg_entsoe_load.sql for details.
    select
        zone,
        period_start::timestamptz
            + (position - 1) * (coalesce(resolution_minutes, 60) * interval '1 minute') as ts,
        price as day_ahead_price,
        loaded_at
    from source
),

deduped as (
    -- Dedup at native (sub-hourly) resolution first -- see stg_entsoe_load.sql.
    select
        *,
        row_number() over (
            partition by zone, ts
            order by loaded_at desc
        ) as rn
    from with_ts
),

hourly as (
    -- Aggregate up to hourly -- see stg_entsoe_load.sql. Day-ahead prices are
    -- usually already hourly, but this keeps behavior correct either way.
    select
        zone,
        date_trunc('hour', ts) as ts,
        avg(day_ahead_price) as day_ahead_price
    from deduped
    where rn = 1
    group by zone, date_trunc('hour', ts)
)

select
    zone || '_' || ts::text as row_key,
    zone,
    ts,
    day_ahead_price
from hourly