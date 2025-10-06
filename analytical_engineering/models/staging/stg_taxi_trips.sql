{{ config(materialized='view') }}
select
  cast(tpep_pickup_datetime as DateTime)              as pickup_ts,
  cast(tpep_dropoff_datetime as Nullable(DateTime))   as dropoff_ts,
  cast(PULocationID as Nullable(UInt16))              as pu_location_id,
  cast(DOLocationID as Nullable(UInt16))              as do_location_id,
  cast(trip_distance as Nullable(Float32))            as trip_distance_mi,
  cast(total_amount as Nullable(Float32))             as total_amount_usd,
  cast(tip_amount as Nullable(Float32))               as tip_amount_usd
from {{ source('raw','taxi_trips') }}
