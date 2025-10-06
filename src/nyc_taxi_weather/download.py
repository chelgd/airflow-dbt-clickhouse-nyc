"""Airflow tasks to download NYC Taxi and NYC weather data.

This module exposes:

- :func:`download_file` – generic streamed downloader.
- :func:`make_taxi_download_task` – factory for monthly NYC taxi Parquet downloads.
- :func:`make_weather_download_task` – factory for monthly Open-Meteo JSON downloads.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator

import requests

def download_file(
    url: str,
    output_path: str
):
    """Stream a remote file to disk.

    Downloads the content at ``url`` in chunks and writes it to ``output_path``.
    Parent folders are created if they do not exist.

    Parameters
    ----------
    url
        HTTP(S) URL of the file to download.
    output_path
        Local filesystem path where the file will be saved.

    Raises
    ------
    requests.HTTPError
        If the HTTP request returns a non-successful status code.
    """
    response = requests.get(url, stream=True)
    response.raise_for_status()
    with open(output_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    print(f"Downloaded {url} → {output_path}")


def make_taxi_download_task(
    dag: DAG,
    year: int,
    month: int,
    output_path: str,
    service="yellow"
):
    """Create a PythonOperator to download a monthly NYC Taxi parquet file.

    Constructs the dataset name (e.g., ``yellow_tripdata_2023-03``), resolves the
    public TLC URL, and saves to ``{output_path}/{dataset}.parquet``.

    Parameters
    ----------
    dag
        The DAG to attach this task to.
    year
        Four-digit year (e.g., ``2023``).
    month
        Month number ``1..12``.
    output_path
        Directory where the Parquet file will be written.
    service
        Taxi service prefix (e.g., ``"yellow"``, ``"green"``, ``"fhv"``).

    Returns
    -------
    airflow.operators.python.PythonOperator
        Configured operator that downloads the target file at runtime.
    """
    month_str = f"{month:02d}"
    dataset = f"{service}_tripdata_{year}-{month_str}"
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset}.parquet"
    output_path = f"{output_path}/{dataset}.parquet"

    return PythonOperator(
        task_id=f"download_{dataset}",
        python_callable=download_file,
        op_kwargs={"url": url, "output_path": output_path},
        dag=dag,
    )


def make_weather_download_task(
    dag: DAG,
    year: int,
    month: int,
    output_path: str
):
    """Create a PythonOperator to download monthly NYC weather (Open-Meteo) JSON.

    Builds a date range from the first day of the month to the first day of the
    next month (exclusive) and requests daily aggregates (max/min temperature,
    precipitation) for New York City in the ``America/New_York`` timezone.

    The file is saved as ``{output_path}/weather_{YYYY-MM}.json``.

    Parameters
    ----------
    dag
        The DAG to attach this task to.
    year
        Four-digit year (e.g., ``2023``).
    month
        Month number ``1..12``.
    output_path
        Directory where the JSON file will be written.

    Returns
    -------
    airflow.operators.python.PythonOperator
        Configured operator that downloads the target file at runtime.
    """
    month_str = f"{month:02d}"
    start_date = f"{year}-{month_str}-01"
    if month == 12:
        end_date = f"{year+1}-01-01"
    else:
        end_date = f"{year}-{month+1:02d}-01"

    url = (
        "https://archive-api.open-meteo.com/v1/archive"
        f"?latitude=40.7128&longitude=-74.0060"
        f"&start_date={start_date}&end_date={end_date}"
        "&daily=temperature_2m_max,temperature_2m_min,precipitation_sum"
        "&timezone=America/New_York"
    )

    output_path = f"{output_path}/weather_{year}-{month_str}.json"

    return PythonOperator(
        task_id=f"download_weather_{year}_{month_str}",
        python_callable=download_file,
        op_kwargs={"url": url, "output_path": output_path},
        dag=dag,
    )