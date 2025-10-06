{{ config(materialized='view') }}
select
  t.pickup_date,
  t.trips,
  t.avg_trip_distance_mi,
  w.temp_max_c,
  w.temp_min_c,
  w.precip_mm
from {{ ref('fact_taxi_daily') }} t
left join {{ ref('stg_weather') }} w
  on w.weather_date = t.pickup_date
