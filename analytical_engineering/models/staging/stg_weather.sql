{{ config(materialized='view') }}
select
  cast(date as Date)                         as weather_date,
  cast(temperature_max as Nullable(Float32)) as temp_max_c,
  cast(temperature_min as Nullable(Float32)) as temp_min_c,
  cast(precipitation as Nullable(Float32))   as precip_mm
from {{ source('raw','weather') }}
