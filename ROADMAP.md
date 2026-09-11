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
- **CI**: GitHub Actions runs Python tests + dbt tests on push.

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

## Phase 5 — Airflow orchestration ✅

Airflow running locally via Docker Compose. `dags/nyc_pipeline_dag.py`:
`resolve_month → download_and_upload_to_s3 → bronze_ingestion →
silver_processing → quality_checks → dbt_run → dbt_test` (reordered from
the plan's original sequence so the cheap Bronze/Silver reconciliation
check fails fast, before wasting time on dbt transforms of already-broken
upstream data). `resolve_month` computes year/month from the run date
(today minus an empirically-set 4-month TLC publish lag) if not given
explicitly, sharing the result via XCom. `retries: 2` /
`retry_delay: 5min` configured in `default_args`.

Two real bugs surfaced and got fixed while wiring this up:

- Databricks Jobs API rejects the flat `notebook_task=` shortcut for
  serverless compute -- it always demands an explicit cluster. Fixed by
  submitting via the `tasks` array (multi-task) format instead, which
  serverless only supports that way.
- `fact_trips`'s incremental filter assumed months always load in
  chronological order (`pickup_date >= max already loaded`). Backfilling
  an older month than what's loaded would pass through Bronze/Silver
  fine but silently never reach `fact_trips` -- and the quality_checks
  macro wouldn't catch it either, since it only reconciles Bronze
  against Silver, not Silver against the dbt layer. Fixed by having the
  DAG pass the exact year/month it's processing to `dbt run`/`dbt test`
  via `--vars`, so `fact_trips` always reprocesses that specific month
  regardless of where it falls chronologically.

Checkpoint: **passed** for the full-chain run (all 7 tasks green,
triggered manually from the Airflow UI). The retry-on-failure half of
the checkpoint was reasoned through rather than force-demonstrated:
manually marking a task "Failed" in the UI is a terminal override that
bypasses Airflow's retry evaluation by design (it doesn't simulate a
real execution failure), and forcing a genuine failure would need a
deliberately broken config swapped in and back out. `retries`/
`retry_delay` are confirmed correctly wired in `default_args` via
Airflow's standard mechanism; not independently re-verified beyond that.

## Phase 6 — Dashboard ✅

Power BI, DirectQuery against the Databricks SQL warehouse (`fact_trips`,
`dim_date`, `dim_location`, `dim_payment_type`). Visuals: trip count by
borough, revenue by month. Saved in Power BI's text-based PBIP format
(`Power BI dashboards/`) for source control instead of a single binary
`.pbix`.

Checkpoint: **passed**. All 4 queries resolve, relationships intact, visuals
render real data.

## Phase 7 — CI ✅

`.github/workflows/ci.yml`, two jobs:
- `pytest`: installs `requirements.txt`, runs `tests/test_download_tlc.py`
  (mocks `requests`/`boto3` — no real network or S3 calls in CI).
- `dbt-test`: installs `dbt-databricks`, writes a `profiles.yml` from
  `DATABRICKS_HOST`/`DATABRICKS_HTTP_PATH`/`DATABRICKS_TOKEN` GitHub Actions
  secrets (a CI-only target, `ci`, pointed at the same
  `nyc_taxi.nyc_taxi_lakehouse` schema real data lives in), then runs
  `dbt test`.

Checkpoint: **passed**. Both `pytest` and `dbt-test` jobs verified green on
a real GitHub Actions run (repo secrets configured). One real bug surfaced
and got fixed: plain `pytest` (how CI invokes it) doesn't add the repo root
to `sys.path` the way `python -m pytest` does locally, so
`tests/test_download_tlc.py` failed with `ModuleNotFoundError: No module
named 'ingestion'` on its first CI run. Fixed with a `pytest.ini` setting
`pythonpath = .`, which works regardless of invocation style.
