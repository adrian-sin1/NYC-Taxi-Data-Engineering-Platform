# NYC-Taxi-Data-Engineering-Platform

A data lakehouse for NYC TLC taxi trip data: a medallion-architecture
(Bronze/Silver/Gold) pipeline built on S3, Databricks + PySpark, Delta Lake,
and dbt.

## Status

Bronze → Silver → Gold pipeline built and validated (Phase 1); dbt star
schema built and tested (Phase 2); incremental loads validated end to end
(Phase 3); scaled to Jan-May 2026 — 5 months, 18,999,282 raw trips, 18M+
valid rows in `fact_trips` (Phase 4); Airflow orchestration running
locally via Docker, full DAG chain validated end to end (Phase 5); Power BI
dashboard built on the Gold tables (Phase 6); GitHub Actions CI verified
green for `pytest` + `dbt test` (Phase 7).
See [ROADMAP.md](ROADMAP.md) for the full phase-by-phase plan.

## Structure

- `ingestion/` — scripts to download TLC data and upload to S3
- `spark_jobs/bronze/` — PySpark Bronze ingestion jobs
- `spark_jobs/silver/` — PySpark Silver cleaning/validation jobs
- `dbt_project/` — dbt models (staging, marts) and tests for Silver → Gold
- `dags/` — Airflow DAGs
- `tests/` — pytest tests for ingestion/Spark helper code
- `.github/workflows/` — CI (pytest + dbt test on push)
- `Power BI dashboards/` — the Power BI report (DirectQuery against the
  Gold tables), saved in PBIP text-based format for source control
- `docker-compose.yaml` — local Airflow (webserver, scheduler, Postgres, Redis)

## Power BI Dashboard

![Power BI dashboard](Power%20BI%20dashboards/Dashboards.png)

## Setup

```bash
python -m venv .venv
source .venv/bin/activate  # or .venv\Scripts\activate on Windows
pip install -r requirements.txt
```

### Airflow (local, via Docker)

```bash
cp .env.example .env   # then fill in the same values you'd use elsewhere
docker compose up airflow-init
docker compose up -d
```
Open [http://localhost:8080](http://localhost:8080), log in with `airflow`/`airflow`.
