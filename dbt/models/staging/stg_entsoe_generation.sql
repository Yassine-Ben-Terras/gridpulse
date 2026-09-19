with source as (
    select * from {{ source('raw', 'entsoe_raw') }}
    where doc_kind = 'generation'
),

with_ts as (
    -- Compute the real timestamp using this row's ACTUAL resolution rather
    -- than assuming 1-hour steps -- see stg_entsoe_load.sql for details.
    select
        zone,
        period_start::timestamptz
            + (position - 1) * (coalesce(resolution_minutes, 60) * interval '1 minute') as ts,
        psr_type,
        quantity as generation_mw,
        loaded_at
    from source
),

deduped as (
    -- Dedup at native (sub-hourly) resolution first -- see stg_entsoe_load.sql.
    select
        *,
        row_number() over (
            partition by zone, ts, psr_type
            order by loaded_at desc
        ) as rn
    from with_ts
),

hourly as (
    -- Aggregate up to hourly -- see stg_entsoe_load.sql.
    select
        zone,
        date_trunc('hour', ts) as ts,
        psr_type,
        avg(generation_mw) as generation_mw
    from deduped
    where rn = 1
    group by zone, date_trunc('hour', ts), psr_type
)

select
    zone || '_' || ts::text || '_' || coalesce(psr_type, 'NA') as row_key,
    zone,
    ts,
    psr_type,
    generation_mw
from hourly