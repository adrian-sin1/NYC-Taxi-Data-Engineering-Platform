# NYC Taxi Data Lakehouse

Medallion-architecture (Bronze/Silver/Gold) data pipeline for NYC TLC taxi trip
data, built on S3, Databricks + PySpark, Delta Lake, and dbt.

## Status

Phase 0 — repo & environment scaffolding. See the build plan for the full
phase-by-phase roadmap (MVP pipeline → dbt modeling → incremental loads →
scaling to multiple months → Airflow orchestration → dashboard → CI/CD).

## Structure

- `ingestion/` — scripts to download TLC data and upload to S3
- `spark_jobs/bronze/` — PySpark Bronze ingestion jobs
- `spark_jobs/silver/` — PySpark Silver cleaning/validation jobs
- `dbt_project/` — dbt models (staging, marts) and tests for Silver → Gold
- `dags/` — Airflow DAGs (added once the manual pipeline works)
- `tests/` — pytest tests for ingestion/Spark helper code
- `.github/workflows/` — CI (pytest + dbt test on push)

## Setup

```bash
python -m venv .venv
source .venv/bin/activate  # or .venv\Scripts\activate on Windows
pip install -r requirements.txt
```
