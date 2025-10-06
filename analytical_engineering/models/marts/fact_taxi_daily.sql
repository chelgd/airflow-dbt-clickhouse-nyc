{{ config(materialized='table') }}
select
  toDate(pickup_ts)                               as pickup_date,
  count()                                         as trips,
  avgIf(trip_distance_mi, trip_distance_mi is not null) as avg_trip_distance_mi
from {{ ref('stg_taxi_trips') }}
group by pickup_date
