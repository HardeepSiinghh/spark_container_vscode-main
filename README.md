# Spark Delta ETL Demo

This repository contains a simple Spark ETL demo using Delta Lake and a Debian-based Docker environment.

## Project structure

- `Dockerfile` - builds a container with Spark, Delta Lake, Python, and test dependencies.
- `src/etl.py` - Spark session creation and revenue ETL logic.
- `tests/test_etl.py` - PyTest-based validation of the ETL transformation.
- `notebooks/` - exploratory notebook for working with Spark in the project.

## What it does

The ETL code runs in a docker env removing the need of setting-up all other libraries in your system. The result is written to Delta Lake.

## Prerequisites

- Docker installed locally
- Git installed
- macOS, Linux, or Windows with Docker support

## Build the Docker image

From the repository root:

```bash
docker build -t spark-delta-demo .
```

## Run the container

Start a container from the built image:

```bash
docker run --rm -it spark-delta-demo
```

The container is configured to stay alive with `tail -f /dev/null`, so you can use it as an interactive environment if needed.

## Run the ETL script inside Docker

1. Start the container with a shell:

```bash
docker run --rm -it -v "$PWD":/workspace -w /workspace spark-delta-demo bash
```

2. Run the ETL module:

```bash
python3 -m src.etl
```

The script will create or overwrite `./revenue_data2` as a Delta table.

## Run tests

Inside the same container shell or locally if dependencies are installed:

```bash
pytest
```

## Notes

- The Docker image installs Spark, Delta Lake, and supporting Python packages including `pyspark`, `delta-spark`, `chispa`, and `pytest`.

- The project uses Spark 3.5.3 and Delta Lake 3.3.2 by default.
