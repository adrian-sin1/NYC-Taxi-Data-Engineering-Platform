# NYC-Taxi-Data-Engineering-Platform

A data lakehouse for NYC TLC taxi trip data: a medallion-architecture
(Bronze/Silver/Gold) pipeline built on S3, Databricks + PySpark, Delta Lake,
and dbt.

## Status

Bronze → Silver → Gold pipeline built and validated (Phase 1); dbt star
schema built and tested (Phase 2); incremental loads validated end to end
(Phase 3); scaled to Jan-May 2026 — 5 months, 18,999,282 raw trips, 18M+
valid rows in `fact_trips`, all consistent and all 10 dbt tests passing
(Phase 4). Next: Airflow orchestration, dashboard, CI/CD.
See [ROADMAP.md](ROADMAP.md) for the full phase-by-phase plan.

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
