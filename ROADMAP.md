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

## Phase 3 — Incremental loads ✅

- Bronze: dynamic partition overwrite on `year`/`month` (`replaceWhere`) —
  re-running the same month replaces just that partition instead of the
  whole table; other months are untouched.
- Silver: `MERGE` into the Silver Delta table keyed on a computed
  `trip_key` hash (no natural trip ID exists in the source data). Falls
  back to a one-time full overwrite if the table predates `trip_key`.
  Quarantine uses the same partition-overwrite approach as Bronze.
- dbt: `fact_trips` (the one genuinely large model) is now
  `materialized='incremental'` with `incremental_strategy='merge'` on
  `trip_id`, filtered to `pickup_date >= max(pickup_date)` already in the
  table. `dim_date`/`dim_location`/`daily_trip_metrics` stay full-rebuild
  tables — small enough that incremental complexity isn't worth it.

Checkpoint: **passed**, validated live on all three legs. Bronze re-run for
the same month left the row count unchanged (3,724,889). Silver re-run left
both the valid table (3,518,537) and quarantine (206,352) unchanged — no
duplication on either. dbt's `fact_trips` full-refresh left its row count
unchanged too, and all 10 dbt tests still pass.

## Phase 4 — Scale to 3-6 months ✅

Scaled to Jan-May 2026 (5 months — August wasn't available yet from TLC,
which only publishes ~2 months behind; June/July also weren't out).
Bronze and Silver were refactored to loop over a `MONTHS` list instead of
one hardcoded year/month, so all 4 new months run in a single notebook
pass instead of manual edit-and-rerun per month.

Two real bugs surfaced and got fixed while scaling up:

- **Silver's `MERGE` had no delete clause.** When a validation rule
  changed (see below), rows that used to be valid but no longer are
  were simply absent from the new computed batch — `MERGE` had nothing
  to match or insert, so the stale rows stayed in Silver forever. Fixed
  with a `whenNotMatchedBySourceDelete`, scoped to the specific
  `year`/`month` partition being reprocessed (unscoped, it would have
  deleted every other month's data too, since a single month's run
  never includes any other month in its source).
- **Location ID validation was `BETWEEN 1 AND 265`**, which assumes
  every integer in that range is a real taxi zone. It isn't — IDs 264
  ("Unknown") and 265 ("Outside of NYC") are placeholder rows in the
  official lookup table. Switched to an existence check against the
  actual lookup table instead of a numeric range (currently a no-op
  numerically, since the lookup has no gaps 1-265, but no longer
  correct by coincidence).
- Also added a pickup-year plausibility check (2020-2027) after finding
  8 rows across the dataset with corrupted pickup timestamps
  (2001/2008/2009) that passed the old `pickup < dropoff` check since
  both dates were equally wrong.

Checkpoint: **passed**. Bronze total across 5 months: 18,999,282 rows.
Silver: 18,086,069 valid + 913,213 quarantined = 18,999,282 (matches
Bronze exactly, confirmed via per-month math). `fact_trips`: 18,086,069,
matching Silver's valid total exactly, all 10 dbt tests pass.

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
