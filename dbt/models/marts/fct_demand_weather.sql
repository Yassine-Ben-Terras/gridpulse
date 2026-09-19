with load as (
    select * from {{ ref('stg_entsoe_load') }}
),

-- Wind and solar PSR type codes per ENTSO-E: B16 = Solar, B19 = Wind Onshore, B18 = Wind Offshore.
-- ENTSO-E omits solar rows entirely for hours with no generation (nighttime)
-- rather than reporting them as zero, so a plain sum(...) filter(...) yields
-- NULL for those hours instead of 0. Left un-coalesced, that NULL then gets
-- dropped from model training entirely (dropna on the target column), so the
-- model never sees a single "solar = 0 at night" example. Coalesce to 0 here
-- since missing generation data for these types genuinely means zero output.
generation_pivot as (
    select
        zone,
        ts,
        coalesce(sum(generation_mw) filter (where psr_type = 'B16'), 0) as solar_mw,
        coalesce(sum(generation_mw) filter (where psr_type in ('B18', 'B19')), 0) as wind_mw,
        sum(generation_mw) as total_generation_mw
    from {{ ref('stg_entsoe_generation') }}
    group by zone, ts
),

price as (
    select * from {{ ref('stg_entsoe_price') }}
),

weather as (
    select * from {{ ref('stg_openmeteo_weather') }}
)

select
    load.zone || '_' || load.ts::text as row_key,
    load.zone,
    load.ts,
    load.load_mw,
    coalesce(generation_pivot.solar_mw, 0) as solar_mw,
    coalesce(generation_pivot.wind_mw, 0) as wind_mw,
    generation_pivot.total_generation_mw,
    price.day_ahead_price,
    weather.temperature_2m,
    weather.wind_speed_10m,
    weather.shortwave_radiation
from load
left join generation_pivot
    on load.zone = generation_pivot.zone and load.ts = generation_pivot.ts
left join price
    on load.zone = price.zone and load.ts = price.ts
left join weather
    on load.zone = weather.zone and load.ts = weather.ts