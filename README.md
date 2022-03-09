# Trips Analysis

Trips Analysis is a project to analyze data use the following tools:

* Airflow: Airflow webserver e Scheduler
* Postgres: Postgres database for Airflow metadata and a model trips database.
* Spark: Spark for processing data
* Jupyter notebook with pyspark for interactive development.

## Installation

Pre-requisite: Docker

For Installation:

```bash
make build
```

For up enviroment:

```bash
make up
```

For stop enviroment:

```bash
make stop
```

For dev enviroment:

```bash
make local
make install-requirements
```