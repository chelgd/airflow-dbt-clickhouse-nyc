CREATE DATABASE IF NOT EXISTS nyc ON CLUSTER ch_cluster_2x2;


CREATE TABLE IF NOT EXISTS nyc.weather_local ON CLUSTER ch_cluster_2x2
(
    `date` Date,
    `temperature_max` Nullable(Float32),
    `temperature_min` Nullable(Float32),
    `precipitation`   Nullable(Float32)
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/{shard}/nyc/weather_local',  -- Keeper path
    '{replica}'                                      -- Replica name
)
PARTITION BY toYYYYMM(date)
ORDER BY date
SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS nyc.taxi_trips_local ON CLUSTER ch_cluster_2x2
(
    `VendorID`               Nullable(UInt8),
    `tpep_pickup_datetime`   DateTime,
    `tpep_dropoff_datetime`  Nullable(DateTime),
    `passenger_count`        Nullable(UInt8),
    `trip_distance`          Nullable(Float32),
    `RatecodeID`             Nullable(UInt8),
    `store_and_fwd_flag`     Nullable(String),
    `PULocationID`           Nullable(UInt16),
    `DOLocationID`           Nullable(UInt16),
    `payment_type`           Nullable(UInt8),
    `fare_amount`            Nullable(Float32),
    `extra`                  Nullable(Float32),
    `mta_tax`                Nullable(Float32),
    `tip_amount`             Nullable(Float32),
    `tolls_amount`           Nullable(Float32),
    `improvement_surcharge`  Nullable(Float32),
    `total_amount`           Nullable(Float32),
    `congestion_surcharge`   Nullable(Float32),
    `airport_fee`            Nullable(Float32)
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/{shard}/nyc/taxi_trips_local',
    '{replica}'
)
PARTITION BY toYYYYMM(tpep_pickup_datetime)
ORDER BY tpep_pickup_datetime
SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS nyc.weather_errors_local ON CLUSTER ch_cluster_2x2
(
    file String,
    raw  String,
    errors String,
    ingested_at DateTime DEFAULT now()
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/{shard}/nyc/weather_errors_local',
    '{replica}'
)
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (ingested_at, file);


CREATE TABLE IF NOT EXISTS nyc.taxi_trips_errors_local ON CLUSTER ch_cluster_2x2
(
    file String,
    raw  String,
    errors String,
    ingested_at DateTime DEFAULT now()
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/{shard}/nyc/taxi_trips_errors_local',
    '{replica}'
)
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (ingested_at, file);


------------------------------------------------------------
-- DISTRIBUTED TABLES (cluster-wide entrypoints)
-- Apps should read/write these via ch-router
------------------------------------------------------------

CREATE TABLE IF NOT EXISTS nyc.weather ON CLUSTER ch_cluster_2x2
AS nyc.weather_local
ENGINE = Distributed(
    ch_cluster_2x2,
    nyc,
    weather_local,
    cityHash64(toYYYYMM(date))
);

CREATE TABLE IF NOT EXISTS nyc.taxi_trips ON CLUSTER ch_cluster_2x2
AS nyc.taxi_trips_local
ENGINE = Distributed(
    ch_cluster_2x2,
    nyc,
    taxi_trips_local,
    cityHash64(toYYYYMM(tpep_pickup_datetime))
);

CREATE TABLE IF NOT EXISTS nyc.weather_errors ON CLUSTER ch_cluster_2x2
AS nyc.weather_errors_local
ENGINE = Distributed(
    ch_cluster_2x2,
    nyc,
    weather_errors_local,
    cityHash64(toYYYYMM(ingested_at))
);

CREATE TABLE IF NOT EXISTS nyc.taxi_trips_errors ON CLUSTER ch_cluster_2x2
AS nyc.taxi_trips_errors_local
ENGINE = Distributed(
    ch_cluster_2x2,
    nyc,
    taxi_trips_errors_local,
    cityHash64(toYYYYMM(ingested_at))
);