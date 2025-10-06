"""Parsers and streaming utilities for NYC Taxi & Weather raw files.

This module provides helpers to:

- Parse Open-Meteo daily weather JSONs into validated :class:`~.models.WeatherRecord`
  rows (:func:`parse_weather_json`).
- Parse full Parquet files into :class:`~.models.TaxiRecord` lists
  (:func:`parse_taxi_parquet`).
- Stream large Parquet files in memory-bounded batches and validate rows on the fly
  (:func:`iter_parquet_batches`).

The functions are designed to cooperate with downstream loaders (e.g., ClickHouse)
and move processed input files to a ``processed`` directory once handled.
"""
from pathlib import Path
from typing import Iterator, List, Optional, Tuple
import json
import shutil
import gc

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel, ValidationError

from .models import TaxiRecord, WeatherRecord


def parse_weather_json(
    downloads_dir_path: str | Path,
    processed_dir_path: str | Path,
) -> Tuple[List[WeatherRecord], List[dict]]:
    """Parse all weather JSON files in a directory.

    Reads every ``*.json`` in ``downloads_dir_path``, extracts daily arrays
    (``time``, ``temperature_2m_max``, ``temperature_2m_min``,
    ``precipitation_sum``), and validates row-wise into
    :class:`~.models.WeatherRecord` instances.

    Each file is moved to ``processed_dir_path`` after parsing, regardless of
    success; malformed payloads are captured as invalid entries.

    Parameters
    ----------
    downloads_dir_path
        Directory containing Open-Meteo archive JSON files.
    processed_dir_path
        Directory where parsed files will be moved.

    Returns
    -------
    tuple[list[WeatherRecord], list[dict]]
        A 2-tuple of ``(valid_rows, invalid_rows)`` where:
        - ``valid_rows`` is a list of :class:`WeatherRecord`.
        - ``invalid_rows`` is a list of diagnostic dicts
          ``{"file": str, "row": dict|None, "errors": list[dict]}``.

    Notes
    -----
    - Array length mismatches across daily fields are treated as file-level errors.
    - Memory is proactively freed via ``del`` and ``gc.collect()`` for long runs.
    """
    downloads_dir = Path(downloads_dir_path)
    processed_dir = Path(processed_dir_path)

    all_valid: List[WeatherRecord] = []
    all_invalid: List[dict] = []

    for file_path in sorted(downloads_dir.glob("*.json")):
        try:
            with file_path.open("r") as f:
                data = json.load(f)
        except Exception as e:
            # Couldn't even read/parse JSON
            all_invalid.append({
                "file": str(file_path),
                "row": None,
                "errors": [{"type": "json_error", "msg": str(e)}],
            })
            # move file even if failed (you can change this behavior if you prefer)
            shutil.move(str(file_path), processed_dir / file_path.name)
            del data
            gc.collect()
            continue

        # Extract arrays
        try:
            daily = data["daily"]
            dates = daily["time"]
            max_temps = daily["temperature_2m_max"]
            min_temps = daily["temperature_2m_min"]
            precipitation = daily["precipitation_sum"]
        except KeyError as e:
            all_invalid.append({
                "file": str(file_path),
                "row": None,
                "errors": [{"type": "key_error", "msg": f"Missing key: {e}"}],
            })
            shutil.move(str(file_path), processed_dir / file_path.name)
            del data, daily, dates, max_temps, min_temps, precipitation
            gc.collect()
            continue

        # Validate lengths (defensive)
        n = len(dates)
        if not (len(max_temps) == len(min_temps) == len(precipitation) == n):
            all_invalid.append({
                "file": str(file_path),
                "row": None,
                "errors": [{"type": "length_mismatch",
                            "msg": f"Arrays have different lengths: time={len(dates)}, "
                                   f"tmax={len(max_temps)}, tmin={len(min_temps)}, prcp={len(precipitation)}"}],
            })
            shutil.move(str(file_path), processed_dir / file_path.name)
            del data, daily, dates, max_temps, min_temps, precipitation
            gc.collect()
            continue

        # Row-wise validation
        valid: List[WeatherRecord] = []
        invalid: List[dict] = []
        for i in range(n):
            row = {
                "date": dates[i],
                "temperature_max": max_temps[i],
                "temperature_min": min_temps[i],
                "precipitation": precipitation[i],
            }
            try:
                valid.append(WeatherRecord(**row))
            except ValidationError as e:
                invalid.append({"file": str(file_path), "row": row, "errors": e.errors()})

        # Aggregate
        all_valid.extend(valid)
        all_invalid.extend(invalid)

        # Move file after processing
        shutil.move(str(file_path), processed_dir / file_path.name)

        del valid, invalid, data, daily, dates, max_temps, min_temps, precipitation
        gc.collect()

    return all_valid, all_invalid


def iter_parquet_batches(
    file_path: str | Path,
    batch_size: int
) -> Iterator[List[BaseModel | dict]]:
    """Stream-validate a Parquet file in Arrow record batches.

    Iterates over ``file_path`` using :class:`pyarrow.parquet.ParquetFile`
    and yields memory-bounded lists of validated Pydantic models along with a
    collection of validation errors for rejected rows.

    Parameters
    ----------
    file_path
        Path to the Parquet file to read.
    batch_size
        Target number of rows per Arrow record batch.

    Yields
    ------
    tuple[list[BaseModel], list[dict]]
        A tuple ``(valid_batch, invalid_batch)`` for each streamed batch, where
        ``invalid_batch`` elements look like
        ``{"file": str, "row": dict, "errors": list[dict]}``.

    Examples
    --------
    >>> for valid, invalid in iter_parquet_batches("yellow.parquet", 100_000):
    ...     insert_somewhere(valid)
    ...     log_errors(invalid)
    """
    pf = pq.ParquetFile(str(file_path))
    # Stream record batches; use small target_rows_per_batch ~= batch_size
    for rb in pf.iter_batches(batch_size=batch_size):
        # rb: pyarrow.RecordBatch
        table = pa.Table.from_batches([rb])
        # convert to a list of row dicts (zero-copy-ish)
        rows = table.to_pylist()

        valid_batch: List[BaseModel] = []
        invalid_batch: List[dict] = []
        for row in rows:
            try:
                valid_batch.append(TaxiRecord(**row))
            except ValidationError as e:
                invalid_batch.append({"file": str(file_path), "row": row, "errors": e.errors()})
            finally:
                del row
                gc.collect()
                

        # Free Arrow objects ASAP
        del rows, table, rb
        gc.collect()

        yield valid_batch, invalid_batch

    # pf closes when GC; explicit del helps
    del pf
    gc.collect()