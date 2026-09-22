-- Minimal but meaningful fixture: two hours of FR data, deliberately designed
-- to exercise the tricky parts of the pipeline, not just "does it run":
--   * position=1 is inserted TWICE with different loaded_at, to verify the
--     staging models' dedup picks the newer value (2000), not the older (1000)
--   * solar (B16) has data for hour 1 only, NOT hour 0 -- hour 0 should come
--     out as an explicit 0 in the mart (coalesced), not NULL
--   * both hours have 4 quarter-hour points (resolution_minutes=15) that must
--     be averaged up into a single hourly row
--
-- Expected results after `dbt run` (see .github/ci/verify_fixture.py):
--   2026-01-01 00:00 : load_mw=1265.0, wind_mw=315.0, solar_mw=0.0,   price=53.0, temp=5.0
--   2026-01-01 01:00 : load_mw=1115.0, wind_mw=355.0, solar_mw=500.0, price=63.0, temp=6.0

insert into raw.entsoe_raw (zone, period_start, position, resolution_minutes, psr_type, quantity, price, doc_kind, loaded_at) values
-- load
('FR','2026-01-01 00:00:00+00',1,15,null,1000,null,'load','2026-01-01 00:05:00+00'),
('FR','2026-01-01 00:00:00+00',1,15,null,2000,null,'load','2026-01-01 00:10:00+00'), -- dedup winner (newer loaded_at)
('FR','2026-01-01 00:00:00+00',2,15,null,1010,null,'load','2026-01-01 00:10:00+00'),
('FR','2026-01-01 00:00:00+00',3,15,null,1020,null,'load','2026-01-01 00:10:00+00'),
('FR','2026-01-01 00:00:00+00',4,15,null,1030,null,'load','2026-01-01 00:10:00+00'),
('FR','2026-01-01 00:00:00+00',5,15,null,1100,null,'load','2026-01-01 00:10:00+00'),
('FR','2026-01-01 00:00:00+00',6,15,null,1110,null,'load','2026-01-01 00:10:00+00'),
('FR','2026-01-01 00:00:00+00',7,15,null,1120,null,'load','2026-01-01 00:10:00+00'),
('FR','2026-01-01 00:00:00+00',8,15,null,1130,null,'load','2026-01-01 00:10:00+00'),
-- wind (B19), both hours
('FR','2026-01-01 00:00:00+00',1,15,'B19',300,null,'generation','2026-01-01 00:10:00+00'),
('FR','2026-01-01 00:00:00+00',2,15,'B19',310,null,'generation','2026-01-01 00:10:00+00'),
('FR','2026-01-01 00:00:00+00',3,15,'B19',320,null,'generation','2026-01-01 00:10:00+00'),
('FR','2026-01-01 00:00:00+00',4,15,'B19',330,null,'generation','2026-01-01 00:10:00+00'),
('FR','2026-01-01 00:00:00+00',5,15,'B19',340,null,'generation','2026-01-01 00:10:00+00'),
('FR','2026-01-01 00:00:00+00',6,15,'B19',350,null,'generation','2026-01-01 00:10:00+00'),
('FR','2026-01-01 00:00:00+00',7,15,'B19',360,null,'generation','2026-01-01 00:10:00+00'),
('FR','2026-01-01 00:00:00+00',8,15,'B19',370,null,'generation','2026-01-01 00:10:00+00'),
-- solar (B16), hour 1 ONLY -- hour 0 deliberately has zero rows
('FR','2026-01-01 00:00:00+00',5,15,'B16',500,null,'generation','2026-01-01 00:10:00+00'),
-- day-ahead price, both hours
('FR','2026-01-01 00:00:00+00',1,15,null,null,50,'day_ahead_price','2026-01-01 00:10:00+00'),
('FR','2026-01-01 00:00:00+00',2,15,null,null,52,'day_ahead_price','2026-01-01 00:10:00+00'),
('FR','2026-01-01 00:00:00+00',3,15,null,null,54,'day_ahead_price','2026-01-01 00:10:00+00'),
('FR','2026-01-01 00:00:00+00',4,15,null,null,56,'day_ahead_price','2026-01-01 00:10:00+00'),
('FR','2026-01-01 00:00:00+00',5,15,null,null,60,'day_ahead_price','2026-01-01 00:10:00+00'),
('FR','2026-01-01 00:00:00+00',6,15,null,null,62,'day_ahead_price','2026-01-01 00:10:00+00'),
('FR','2026-01-01 00:00:00+00',7,15,null,null,64,'day_ahead_price','2026-01-01 00:10:00+00'),
('FR','2026-01-01 00:00:00+00',8,15,null,null,66,'day_ahead_price','2026-01-01 00:10:00+00');

insert into raw.openmeteo_raw (zone, time, temperature_2m, wind_speed_10m, shortwave_radiation, loaded_at) values
('FR','2026-01-01 00:00:00+00',5.0,10.0,0.0,'2026-01-01 00:10:00+00'),
('FR','2026-01-01 01:00:00+00',6.0,11.0,100.0,'2026-01-01 00:10:00+00');