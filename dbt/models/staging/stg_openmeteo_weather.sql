with source as (
    select * from {{ source('raw', 'openmeteo_raw') }}
),

deduped as (
    select
        zone,
        time::timestamptz as ts,
        temperature_2m,
        wind_speed_10m,
        shortwave_radiation,
        loaded_at,
        row_number() over (
            partition by zone, time
            order by loaded_at desc
        ) as rn
    from source
)

select
    zone || '_' || ts::text as row_key,
    zone,
    ts,
    temperature_2m,
    wind_speed_10m,
    shortwave_radiation
from deduped
where rn = 1
