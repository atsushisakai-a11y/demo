# ============================================
# DAG: Run BigQuery SQL from GitHub (fixed)
# ============================================
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import requests
import os

# ----------------------------
# Config
# ----------------------------
GITHUB_REPO = "atsushisakai-a11y/google-maps-bq-pipeline"
SQL_FILES = ["sql/transform.sql", "sql/datamart.sql"]
BRANCH = "main"

PROJECT_ID = "grand-water-473707-r8"
DATASET_ID = "osm-demo"
LOCATION = "europe-west4"

# ----------------------------
# DAG Definition
# ----------------------------
default_args = {
    "owner": "atsushi",
    "start_date": days_ago(1),
    "retries": 1,
}

with DAG(
    dag_id="run_bq_sql_from_github",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["bigquery", "github", "google-maps"],
) as dag:

    def fetch_and_run_sql():
        for sql_file in SQL_FILES:
            raw_url = f"https://raw.githubusercontent.com/{GITHUB_REPO}/{BRANCH}/{sql_file}"
            print(f"🔗 Fetching SQL from: {raw_url}")
            resp = requests.get(raw_url)
            resp.raise_for_status()
            sql = resp.text
            print(f"✅ Running {sql_file} ({len(sql)} chars) in BigQuery...")

            from google.cloud import bigquery
            client = bigquery.Client(project=PROJECT_ID)
            job = client.query(sql)
            job.result()
            print(f"🎉 Finished executing {sql_file}")

    run_bq_pipeline = PythonOperator(
        task_id="run_bq_pipeline",
        python_callable=fetch_and_run_sql,
    )
