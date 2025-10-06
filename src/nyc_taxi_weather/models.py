"""Data models for NYC weather and taxi records.

This module defines Pydantic v2 models used in parsing/validation before
loading into ClickHouse.

Models
------
WeatherRecord
    Daily weather metrics from Open-Meteo (one row per date).
TaxiRecord
    NYC Yellow Taxi trip record (one row per trip).
"""
from datetime import date, datetime
from typing import Optional
from pydantic import BaseModel, field_validator

class WeatherRecord(BaseModel):
    """Validated daily weather record.

    Parameters
    ----------
    date
        Calendar date (parsed from ISO string if provided as ``str``).
    temperature_max
        Daily maximum temperature in degrees Celsius.
    temperature_min
        Daily minimum temperature in degrees Celsius.
    precipitation
        Daily total precipitation in millimeters.
    """
    date: date
    temperature_max: Optional[float] = None
    temperature_min: Optional[float] = None
    precipitation: Optional[float] = None

    @field_validator("date", mode="before")
    @classmethod
    def _parse_date(cls, v):
        return date.fromisoformat(v) if isinstance(v, str) else v


class TaxiRecord(BaseModel):
    """Validated NYC Yellow Taxi trip record.

    Fields mirror the public NYC TLC trip schema for Yellow cabs. Datetime
    fields are parsed from ISO strings when necessary.

    Parameters
    ----------
    VendorID
        TPEP provider code.
    tpep_pickup_datetime
        Trip pickup timestamp (UTC or local per source — kept as provided).
    tpep_dropoff_datetime
        Trip dropoff timestamp.
    passenger_count
        Number of passengers.
    trip_distance
        Trip distance (miles).
    RatecodeID
        Final rate code in effect at the end of the trip.
    store_and_fwd_flag
        ``"Y"`` if held in vehicle memory before sending to vendor; else ``"N"``.
    PULocationID
        TLC taxi zone ID for pickup.
    DOLocationID
        TLC taxi zone ID for dropoff.
    payment_type
        Code for payment type (e.g. 1=Credit card, 2=Cash).
    fare_amount
        Metered fare.
    extra
        Misc extras (e.g., surcharge).
    mta_tax
        MTA tax amount.
    tip_amount
        Tip amount.
    tolls_amount
        Tolls amount.
    improvement_surcharge
        Improvement surcharge amount.
    total_amount
        Total charged to passenger.
    congestion_surcharge
        Congestion surcharge amount.
    airport_fee
        Airport fee amount.
    """
    VendorID: Optional[int] = None
    tpep_pickup_datetime: datetime
    tpep_dropoff_datetime: Optional[datetime] = None
    passenger_count: Optional[int] = None
    trip_distance: Optional[float] = None
    RatecodeID: Optional[int] = None
    store_and_fwd_flag: Optional[str] = None
    PULocationID: Optional[int] = None
    DOLocationID: Optional[int] = None
    payment_type: Optional[int] = None
    fare_amount: Optional[float] = None
    extra: Optional[float] = None
    mta_tax: Optional[float] = None
    tip_amount: Optional[float] = None
    tolls_amount: Optional[float] = None
    improvement_surcharge: Optional[float] = None
    total_amount: Optional[float] = None
    congestion_surcharge: Optional[float] = None
    airport_fee: Optional[float] = None

    @field_validator("tpep_pickup_datetime", "tpep_dropoff_datetime", mode="before")
    @classmethod
    def _parse_dt(cls, v):
        return datetime.fromisoformat(v) if isinstance(v, str) else v
