with actuals_wide as (
    select zone, ts, load_mw as demand, wind_mw as wind_generation, solar_mw as solar_generation
    from {{ ref('fct_demand_weather') }}
),

-- Unpivot to match forecasts' narrow (zone, ts, target, value) shape
actuals_long as (
    select zone, ts, 'demand' as target, demand as actual_value from actuals_wide
    union all
    select zone, ts, 'wind_generation' as target, wind_generation as actual_value from actuals_wide
    union all
    select zone, ts, 'solar_generation' as target, solar_generation as actual_value from actuals_wide
),

forecasts as (
    select * from {{ source('app', 'forecasts') }}
),

-- Freshest prediction per (zone, ts, target) -- multiple model_versions or
-- retrigger runs can produce more than one row for the same target hour;
-- keep the one computed most recently.
latest_forecasts as (
    select
        *,
        row_number() over (
            partition by zone, ts, target
            order by predicted_at desc
        ) as rn
    from forecasts
)

-- Inner join: only rows where the actual has since arrived (i.e. the
-- predicted hour has actually happened and been ingested) show up here.
-- Anything still in the future is simply absent, not a null row.
select
    f.zone,
    f.ts,
    f.target,
    f.predicted_value,
    a.actual_value,
    f.predicted_value - a.actual_value as error,
    abs(f.predicted_value - a.actual_value) as abs_error,
    case
        when a.actual_value != 0 then abs(f.predicted_value - a.actual_value) / abs(a.actual_value) * 100
        else null
    end as abs_pct_error,
    f.model_version,
    f.predicted_at
from latest_forecasts f
inner join actuals_long a
    on f.zone = a.zone and f.ts = a.ts and f.target = a.target
where f.rn = 1