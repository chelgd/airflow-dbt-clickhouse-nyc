"""
NYC Taxi & Weather Ingestion DAG
===============================

This DAG downloads NYC Yellow Taxi Parquet files and Open-Meteo daily
weather JSON, then parses and loads them into ClickHouse. Taxi files are
ingested in **streaming batches** to keep memory usage low.

Prerequisites
-------------
- ClickHouse reachable (e.g., via HAProxy router).
- Writable volumes for ``/data/downloads`` and ``/data/processed``.
- Python deps installed for operators and the project code.

Environment Variables
---------------------
- ``CLICKHOUSE_HOST``: ClickHouse host (e.g., ``ch-router``).
- ``CLICKHOUSE_PORT``: ClickHouse port (default: ``19000``).
- ``CLICKHOUSE_USER``: ClickHouse username.
- ``CLICKHOUSE_PASSWORD``: ClickHouse password.
- ``CLICKHOUSE_DB``: Target database/schema name.

Data Paths
----------
- Taxi downloads: ``/data/downloads/taxi``
- Weather downloads: ``/data/downloads/weather``
- Processed taxi: ``/data/processed/taxi``
- Processed weather: ``/data/processed/weather``

Notes
-----
- The *download* tasks for taxi and weather run in parallel.
- After both finish, parsing/loading tasks run (also in parallel).
- The batch size for taxi inserts can be tuned via ``taxi_batch_size``.
"""
import sys, os
from pathlib import Path
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator


sys.path.append(Path.joinpath(Path(__file__).parent.parent, "src").as_posix())

from nyc_taxi_weather.download import (
    make_taxi_download_task, 
    make_weather_download_task
)

from nyc_taxi_weather.load import (
    process_taxi_files,
    process_weather_files
)

# This would usually come from Airflow Connections/Variables or a Secrets Manager
# For simplicity, we use environment variables here
clickhouse_con = {
    "host": os.getenv("CLICKHOUSE_HOST", ""),
    "port": int(os.getenv("CLICKHOUSE_PORT", 19000)),
    "user": os.getenv("CLICKHOUSE_USER", ""),
    "password": os.getenv("CLICKHOUSE_PASSWORD", ""),
    "database": os.getenv("CLICKHOUSE_DB", ""),
}


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

TAXI_DOWNLOADS_DIR = "/data/downloads/taxi"
TAXI_PROCESSED_DIR = "/data/processed/taxi"
WEATHER_DOWNLOADS_DIR = "/data/downloads/weather"
WEATHER_PROCESSED_DIR = "/data/processed/weather"

with DAG(
    dag_id=Path(__file__).stem,
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["nyc", "taxi_trips", "weather", "parallel"],
) as dag:

    dag.doc_md = """
    NYC Taxi & Weather Ingestion DAG
    ===============================

    **Purpose**
      Orchestrates parallel downloading of NYC Taxi and NYC weather data,
      then parses and loads the results into ClickHouse.

    **Flow**
      #. Download taxi Parquet and weather JSON in parallel.
      #. Wait for both to complete.
      #. Parse + load weather (single batch).
      #. Stream-parse taxi Parquet and load in configurable batches.

    **Key Parameters**
      - *ClickHouse*: read from environment variables.
      - *Taxi batch size*: set via ``taxi_batch_size`` in the ``process_taxi_files`` task.

    **Storage Layout**
      - Downloads: ``/data/downloads/{taxi,weather}``
      - Processed: ``/data/processed/{taxi,weather}``

    **Reliability**
      - Minimal retries on network downloads.
      - Memory-safe streaming for large Parquet files.
    """

    # Example: Download 3 months of data in parallel
    taxi_tasks = []
    weather_tasks = []
    for year, month in [(2024, 6)]:
        taxi_task = make_taxi_download_task(dag, year, month, TAXI_DOWNLOADS_DIR, service="yellow")
        taxi_tasks.append(taxi_task)

        weather_task = make_weather_download_task(dag, year, month, WEATHER_DOWNLOADS_DIR)
        weather_tasks.append(weather_task)

    process_weather_task = PythonOperator(
        task_id="process_weather_files",
        python_callable=process_weather_files,
        op_kwargs={
            "dag": dag,  # your function signature accepts this; not used
            "downloads_dir": WEATHER_DOWNLOADS_DIR,
            "processed_dir": WEATHER_PROCESSED_DIR,
            "clickhouse_con": clickhouse_con,
        },
        execution_timeout=timedelta(hours=1),
    )
    process_weather_task.doc_md = """
    Process Weather Files
    ---------------------

    **Steps**
      #. Parse all Open-Meteo JSON in ``/data/downloads/weather``.
      #. Validate into Pydantic models.
      #. Insert into ClickHouse table ``weather`` (single batch).
      #. Move processed files to ``/data/processed/weather``.

    **Failure Handling**
      Invalid rows are collected and can be redirected to an error table
      (handled in the parsing/loading layer if configured).
    """

    process_taxi_task = PythonOperator(
        task_id="process_taxi_files",
        python_callable=process_taxi_files,
        op_kwargs={
            "dag": dag,
            "downloads_dir": TAXI_DOWNLOADS_DIR,
            "processed_dir": TAXI_PROCESSED_DIR,
            "clickhouse_con": clickhouse_con,
            "taxi_batch_size": 500,
        },
        execution_timeout=timedelta(hours=6),
    )
    process_taxi_task.doc_md = """
    Process Taxi Files (Streaming)
    ------------------------------

    **Steps**
      #. Iterate Parquet files in ``/data/downloads/taxi``.
      #. Stream-parse into Arrow record batches (no full file in RAM).
      #. Validate to Pydantic models.
      #. Insert into ClickHouse table ``taxi_trips`` in batches of
         ``taxi_batch_size`` rows (default: 1,000).
      #. Move each processed file to ``/data/processed/taxi``.

    **Tuning**
      Increase ``taxi_batch_size`` to improve throughput; decrease if memory
      pressure is observed.
    """

    join_downloads = EmptyOperator(task_id="join_downloads")

    (taxi_tasks + weather_tasks) >> join_downloads
    join_downloads >> [process_weather_task, process_taxi_task]