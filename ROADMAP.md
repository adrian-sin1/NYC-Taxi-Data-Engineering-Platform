# Build Plan

Phase-by-phase roadmap for the NYC Taxi Data Engineering Platform. Each phase
has a checkpoint that must pass before moving to the next.

## Locked-in decisions

- **Storage**: AWS S3 for raw data, Delta Lake for Bronze/Silver/Gold tables.
- **Processing**: Databricks + PySpark for Bronze/Silver (distributed
  cleaning, validation, joins). dbt for Silver→Gold SQL modeling + tests —
  dbt sits on top of PySpark, doesn't replace it.
- **Orchestration**: Airflow, added after the manual pipeline works once by
  hand.
- **Incremental loads**: Silver and Gold use Delta `MERGE`/partition-based
  upserts, not full reprocessing on every run — matters once Airflow is
  scheduling this monthly.
- **Scope**: 3-6 months of NYC TLC taxi trip data to start.
- **Data quality**: explicit validation rules (`trip_distance > 0`,
  `fare_amount >= 0`, `pickup < dropoff`, valid `location_id`), invalid
  records routed to a quarantine table (not silently dropped).
- **CI/CD**: GitHub Actions runs Python tests + dbt tests on push.

## Phase 0 — Repo & environment setup ✅

Folder structure, `.gitignore`, S3 bucket (`nyc-transportation-adrian`),
Databricks Free Edition workspace with Unity Catalog external location for
S3 access, taxi-zone lookup CSV downloaded.

## Phase 1 — MVP: one month, Bronze → Silver → one Gold table, run by hand ✅

- `ingestion/download_tlc.py` — pulls a month of TLC parquet, uploads to S3.
- `spark_jobs/bronze/ingest_taxi.py` — raw parquet → Bronze Delta table,
  stamped with `ingestion_timestamp`/`source_file`.
- `spark_jobs/silver/clean_taxi.py` — dedupe, quarantine invalid rows,
  compute `trip_duration`/`pickup_hour`/`fare_per_mile`, join to taxi-zone
  lookup.
- One-off Gold aggregation proving the chain works end to end.

Checkpoint: queried `daily_trip_metrics` for Jan 2026 — sane trip counts,
revenue, avg fare. **Passed.**

## Phase 2 — dbt for Silver → Gold ✅

dbt project against the Databricks SQL warehouse. Star schema: `dim_date`,
`dim_location`, `dim_payment_type` (seed), `fact_trips` (grain: one row per
trip), `daily_trip_metrics`. Tests: uniqueness/not-null on `trip_id`,
referential integrity to dimension tables, `fare_amount >= 0`.

Checkpoint: `dbt run && dbt test` passes cleanly. **Passed.**

## Phase 3 — Incremental loads

- Bronze: append-only, partitioned by `year`/`month`.
- Silver: `MERGE` new Bronze records instead of reprocessing the whole
  table.
- dbt: incremental materializations for the larger Gold/staging models.

Checkpoint: running the pipeline twice on the same month doesn't duplicate
or reprocess unnecessarily.

## Phase 4 — Scale to 3-6 months

Repeat Phase 1's ingestion for the remaining months, relying on Phase 3's
incremental logic so each new month is additive.

## Phase 5 — Airflow orchestration

`dags/nyc_pipeline_dag.py`: `download_data → upload_to_s3 →
bronze_ingestion → silver_processing → dbt_run → dbt_test →
quality_checks`. Retries and failure alerting on Databricks job tasks.
Scheduled monthly.

Checkpoint: trigger the DAG manually, watch it run the full chain; kill a
task mid-run to confirm retry behavior.

## Phase 6 — Dashboard

Tableau, Power BI, or a lightweight alternative (Streamlit / Databricks SQL
dashboard) on the Gold tables: trips-by-hour, trips-by-borough,
revenue-by-month.

## Phase 7 — CI/CD

`.github/workflows/ci.yml`: install deps → `pytest` → `dbt test` (CI-safe
target) → report pass/fail. Not a full deployment pipeline.

## Phase 8 — README / architecture writeup

Problem statement, architecture diagram, "why each technology" table (S3,
Airflow, Databricks, PySpark, Delta Lake, dbt, BI tool, GitHub Actions).
