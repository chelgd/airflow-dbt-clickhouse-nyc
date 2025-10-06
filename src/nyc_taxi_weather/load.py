"""
Ingestion utilities for streaming NYC Taxi & Weather datasets into ClickHouse.

This module provides:
- Batch-friendly inserts into ClickHouse (:func:`load_to_ch`)
- Streaming, memory-safe Parquet ingestion for taxi files
  (:func:`process_single_taxi_file_streaming`, :func:`process_taxi_files`)
- Weather JSON parsing + loading orchestration (:func:`process_weather_files`)
"""

from typing import List
from pathlib import Path
import shutil
import gc
from pydantic import BaseModel
from clickhouse_driver import Client
from airflow import DAG

from .parse import (
    parse_weather_json,
    iter_parquet_batches,
)


def load_to_ch(
    data: List[BaseModel],
    table: str,
    clickhouse_con: dict,
    batch_size: int = 50_000,
) -> int:
    """
    Insert Pydantic models into a ClickHouse table in sub-batches.

    This function converts a list of Pydantic model instances to dictionaries
    and performs one or more ``INSERT INTO`` statements using
    :mod:`clickhouse_driver`. Splitting the payload into sub-batches helps
    bound memory usage and improve throughput.

    Parameters
    ----------
    data
        List of validated Pydantic model instances to insert.
    table
        Fully qualified ClickHouse table name (e.g. ``"nyc.taxi_trips"`` or
        a Distributed table alias).
    clickhouse_con
        Connection kwargs passed to :class:`clickhouse_driver.Client`, e.g.
        ``{"host": "ch-router", "port": 19000, "user": "...", ...}``.
    batch_size
        Maximum number of rows per insert statement.

    Returns
    -------
    int
        Total number of rows inserted.

    Notes
    -----
    - ClickHouse does not require an explicit ``commit()`` for inserts.
    - Consider enabling deduplication settings on the server or per query
      if you implement application-level retries.

    Examples
    --------
    >>> rows_inserted = load_to_ch(models, "nyc.taxi_trips", ch_kwargs, batch_size=100_000)
    >>> print(rows_inserted)
    250000
    """
    if not data:
        return 0

    client = Client(**clickhouse_con)

    inserted = 0
    for i in range(0, len(data), batch_size):
        chunk = data[i:i+batch_size]
        records = [m.model_dump() if hasattr(m, "model_dump") else m.dict() for m in chunk]
        cols = list(records[0].keys())
        client.execute(f"INSERT INTO {table} ({', '.join(cols)}) VALUES", records)
        inserted += len(records)
        del records, chunk, cols
        gc.collect()

    return inserted


def process_single_taxi_file_streaming(
    file_path: str | Path,
    processed_dir: str | Path,
    clickhouse_con: dict,
    batch_size: int = 100_000
) -> int:
    """
    Stream-parse a single Parquet taxi file and insert rows into ClickHouse.

    The file is read in Arrow record batches via ``iter_parquet_batches`` to
    avoid loading the entire Parquet into memory. Each yielded batch is a list
    of validated :class:`~nyc_taxi_weather.models.TaxiRecord` instances and is
    inserted using :func:`load_to_ch`. After processing, the file is moved to
    ``processed_dir`` regardless of success.

    Parameters
    ----------
    file_path
        Path to the Parquet file to process.
    processed_dir
        Directory where the file will be moved after processing.
    clickhouse_con
        Connection kwargs for :class:`clickhouse_driver.Client`.
    batch_size
        Maximum number of rows per ClickHouse insert.
    columns
        Optional subset of columns to read from Parquet. If provided, the
        reader will project only these fields for lower I/O and memory.

    Returns
    -------
    int
        Total number of rows inserted from this file.

    See Also
    --------
    iter_parquet_batches :
        Yields tuples of (valid_batch, invalid_batch) for a Parquet file.
    load_to_ch :
        Inserts a batch of Pydantic models into ClickHouse.

    Examples
    --------
    >>> n = process_single_taxi_file_streaming(
    ...     "/data/downloads/taxi/yellow_tripdata_2024-06.parquet",
    ...     "/data/processed/taxi",
    ...     ch_kwargs,
    ...     batch_size=200_000,
    ... )
    >>> print(n)
    3066766
    """
    file_path = Path(file_path)
    processed_dir = Path(processed_dir)
    processed_dir.mkdir(parents=True, exist_ok=True)

    total_inserted = 0
    try:
        for valid_batch, invalid_batch in iter_parquet_batches(file_path, batch_size=batch_size):
            total_inserted += load_to_ch(valid_batch, "nyc.taxi_trips", clickhouse_con, batch_size=batch_size)
            del valid_batch, invalid_batch
            gc.collect()
    finally:
        shutil.move(str(file_path), processed_dir / file_path.name)

    return total_inserted


# ---------- Orchestrators used by DAG operator ----------

def process_taxi_files(
    dag: DAG,
    downloads_dir: str,
    processed_dir: str,
    clickhouse_con: dict,
    taxi_batch_size: int = 10_000,
) -> None:
    """
    Stream and load **all** Parquet files in a directory into ClickHouse.

    Iterates over ``downloads_dir`` for files matching ``*.parquet``, and for
    each one calls :func:`process_single_taxi_file_streaming`. Designed to be
    used directly as an Airflow PythonOperator callable.

    Parameters
    ----------
    dag
        The active :class:`airflow.DAG`. Not used at runtime; kept for parity
        with Airflow operator signatures.
    downloads_dir
        Directory containing Parquet files to process.
    processed_dir
        Directory where processed files will be moved.
    clickhouse_con
        Connection kwargs for :class:`clickhouse_driver.Client`.
    taxi_batch_size
        Batch size (rows per insert) passed through to
        :func:`process_single_taxi_file_streaming`.

    Returns
    -------
    None

    Notes
    -----
    - Files are processed in sorted filename order.
    - If a file yields no valid rows, it is still moved to ``processed_dir``.
    """

    files = sorted(Path(downloads_dir).glob("*.parquet"))
    if not files:
        print("[taxi_trips] No files to process")
        return

    total = 0
    for fp in files:
        cnt = process_single_taxi_file_streaming(
            fp,
            processed_dir,
            clickhouse_con,
            batch_size=taxi_batch_size
        )
        total += cnt
        print(f"[taxi_trips] {fp.name}: inserted {cnt} rows")

    print(f"[taxi_trips] TOTAL inserted: {total}")


def process_weather_files(
    dag: DAG,
    downloads_dir: str,
    processed_dir: str,
    clickhouse_con: dict,
) -> None:
    """
    Parse all weather JSON files in a directory and load them into ClickHouse.

    This function reads all JSON files under ``downloads_dir`` via
    :func:`parse_weather_json`, then performs a single batched insert into the
    ``weather`` table using :func:`load_to_ch`. Files are moved to
    ``processed_dir`` by the parser.

    Parameters
    ----------
    dag
        The active :class:`airflow.DAG`. Included for operator parity.
    downloads_dir
        Directory with Open-Meteo daily JSON payloads.
    processed_dir
        Directory where processed JSON files will be moved.
    clickhouse_con
        Connection kwargs for :class:`clickhouse_driver.Client`.

    Returns
    -------
    None

    Examples
    --------
    >>> process_weather_files(
    ...     dag,
    ...     "/data/downloads/weather",
    ...     "/data/processed/weather",
    ...     ch_kwargs,
    ... )
    """
    valid_data, _ = parse_weather_json(downloads_dir, processed_dir)
    if valid_data:
        inserted = load_to_ch(valid_data, "weather", clickhouse_con, batch_size=len(valid_data))
        print(f"[weather] Inserted {inserted} rows")
    else:
        print("[weather] No valid rows parsed")