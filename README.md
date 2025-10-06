## Overview

This project showcases a modern data engineering workflow using [Apache Airflow](https://airflow.apache.org/), [dbt](https://www.getdbt.com/), and [ClickHouse](https://clickhouse.com/). It ingests, transforms, and analyzes NYC taxi trip data, providing a practical example for data engineers.

## Features

- **Automated ETL**: Orchestrate data pipelines with Airflow.
- **Data Modeling**: Transform and model data using dbt.
- **Analytics**: Store and query data efficiently in ClickHouse..

## Required components

- Airflow[CeleryExecutors]
- dbt
- ClickHouse cluster 2_x_2

## Getting Started

### Prerequisites

- Docker & Docker Compose
- Git

### Running the Pipeline

- Trigger the DAG in Airflow to start the ETL process. http://localhost:8080/
- dbt models are meant to be started manually. 
    * dbt debug (check connectivity and debug models)
    * dbt run (run the models)

## Project Structure

- `dags/`: Airflow level orchestration
- `analytical_engineering/`: dbt project for data modeling
- `init_db`: ClickHouse database initialization scripts
- `src/`: Project src code base. Modules and reusable components.

## Resources

- [Airflow Documentation](https://airflow.apache.org/docs/)
- [dbt Documentation](https://docs.getdbt.com/docs/introduction)
- [ClickHouse Documentation](https://clickhouse.com/docs/en/)

## License

MIT License
