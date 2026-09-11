"""NYC Taxi Lakehouse pipeline: download -> upload to S3 -> Bronze ->
Silver -> dbt run -> dbt test -> quality checks, for one month at a time.

Which month gets processed:
- Trigger with "year"/"month" set in the run config to process that exact
  month (backfills, reprocessing, or the Phase 5 manual-trigger checkpoint).
- Trigger with year/month left blank (including automatic @monthly runs)
  and it's computed from the run's logical date minus PUBLISH_LAG_MONTHS,
  matching how far behind TLC actually publishes data. That lag isn't a
  documented constant -- it's set from an empirical check on 2026-09-09,
  where the most recent available month was May 2026 while the check
  itself ran in September (a 4-month gap). Adjust PUBLISH_LAG_MONTHS if
  TLC's publish cadence turns out to be different going forward.

Requires, outside this repo:
- An Airflow connection named "databricks_default" (host + personal access
  token), created via the Airflow UI: Admin -> Connections.
- BRONZE_NOTEBOOK_PATH / SILVER_NOTEBOOK_PATH below pointed at two
  permanent notebooks you've saved in your Databricks workspace, containing
  the current contents of spark_jobs/bronze/ingest_taxi.py and
  spark_jobs/silver/clean_taxi.py respectively.
- CLASSIC_CLUSTER_ID below pointed at a running (or auto-restartable)
  classic cluster in the same workspace -- Bronze/Silver run there instead
  of on serverless job compute.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta

from dateutil.relativedelta import relativedelta

from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import (
    DatabricksSubmitRunOperator,
)

sys.path.insert(0, "/opt/airflow/project")

BRONZE_NOTEBOOK_PATH = "/Workspace/bronze_ingest_taxi"
SILVER_NOTEBOOK_PATH = "/Workspace/silver_clean_taxi"

# Classic multi-node/autoscaling cluster in the Premium workspace, tested
# manually (ran both notebooks against an already-loaded month, row counts
# matched serverless exactly). Used instead of serverless job compute so
# Bronze/Silver get real worker autoscaling.
CLASSIC_CLUSTER_ID = "0910-225254-rijork74"

DBT_PROJECT_DIR = "/opt/airflow/project/dbt_project"

PUBLISH_LAG_MONTHS = 4


def _resolve_year_month(**context):
    params = context["params"]
    year = params.get("year")
    month = params.get("month")

    if not year or not month:
        target = context["logical_date"] - relativedelta(months=PUBLISH_LAG_MONTHS)
        year, month = target.year, target.month

    context["ti"].xcom_push(key="year", value=int(year))
    context["ti"].xcom_push(key="month", value=int(month))


def _download_and_upload(**context):
    from ingestion.download_tlc import download_and_upload

    year = context["ti"].xcom_pull(task_ids="resolve_month", key="year")
    month = context["ti"].xcom_pull(task_ids="resolve_month", key="month")
    download_and_upload(year, month)


YEAR_XCOM = "{{ ti.xcom_pull(task_ids='resolve_month', key='year') }}"
MONTH_XCOM = "{{ ti.xcom_pull(task_ids='resolve_month', key='month') }}"


with DAG(
    dag_id="nyc_pipeline_dag",
    description="Bronze -> Silver -> dbt for one month of NYC taxi data",
    start_date=datetime(2026, 1, 1),
    schedule="@monthly",
    catchup=False,
    params={
        "year": Param(None, type=["null", "integer"]),
        "month": Param(None, type=["null", "integer"]),
    },
    default_args={
        "owner": "nyc_lakehouse",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["nyc_taxi_lakehouse"],
) as dag:
    resolve_month = PythonOperator(
        task_id="resolve_month",
        python_callable=_resolve_year_month,
    )

    download_data = PythonOperator(
        task_id="download_and_upload_to_s3",
        python_callable=_download_and_upload,
    )

    bronze_ingestion = DatabricksSubmitRunOperator(
        task_id="bronze_ingestion",
        databricks_conn_id="databricks_default",
        tasks=[
            {
                "task_key": "bronze_ingestion",
                "existing_cluster_id": CLASSIC_CLUSTER_ID,
                "notebook_task": {
                    "notebook_path": BRONZE_NOTEBOOK_PATH,
                    "base_parameters": {"year": YEAR_XCOM, "month": MONTH_XCOM},
                },
            }
        ],
    )

    silver_processing = DatabricksSubmitRunOperator(
        task_id="silver_processing",
        databricks_conn_id="databricks_default",
        tasks=[
            {
                "task_key": "silver_processing",
                "existing_cluster_id": CLASSIC_CLUSTER_ID,
                "notebook_task": {
                    "notebook_path": SILVER_NOTEBOOK_PATH,
                    "base_parameters": {"year": YEAR_XCOM, "month": MONTH_XCOM},
                },
            }
        ],
    )

    DBT_VARS_ARG = (
        "--vars '{\"year\": " + YEAR_XCOM + ', "month": ' + MONTH_XCOM + "}'"
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd " + DBT_PROJECT_DIR + " && dbt run " + DBT_VARS_ARG,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd " + DBT_PROJECT_DIR + " && dbt test " + DBT_VARS_ARG,
    )

    quality_checks = BashOperator(
        task_id="quality_checks",
        bash_command=(
            "cd " + DBT_PROJECT_DIR + " && dbt run-operation quality_check_month "
            "--args '{\"year\": " + YEAR_XCOM + ', "month": ' + MONTH_XCOM + "}'"
        ),
    )

    (
        resolve_month
        >> download_data
        >> bronze_ingestion
        >> silver_processing
        >> quality_checks
        >> dbt_run
        >> dbt_test
    )
