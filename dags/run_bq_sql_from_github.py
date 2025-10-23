# ============================================
# DAG: Run BigQuery SQL from GitHub
# ============================================
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import requests
import os

# ----------------------------
# Config
# ----------------------------
GITHUB_REPO = "atsushisakai-a11y/google-maps-bq-pipeline"
SQL_FOLDER = "sql"
SQL_FILES = [
    "sql/transform.sql",
    "sql/datamart.sql"
]
BRANCH = "main"

PROJECT_ID = "grand-water-473707-r8"
DATASET_ID = "osm-demo"
LOCATION = "europe-west4"  # adjust if needed

# ----------------------------
# DAG Definition
# ----------------------------
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 1,
}

dag = DAG(
    dag_id="run_bq_sql_from_github",
    default_args=default_args,
    schedule_interval="@daily",  # run daily
    catchup=False,
    tags=["bigquery", "github", "google-maps"],
)

# ----------------------------
# Helper: fetch SQL from GitHub
# ----------------------------
def fetch_sql_from_github():
    raw_url = f"https://raw.githubusercontent.com/{GITHUB_REPO}/{BRANCH}/{SQL_FOLDER}/{SQL_FILE}"
    print(f"🔗 Fetching SQL from: {raw_url}")
    response = requests.get(raw_url)
    response.raise_for_status()
    sql = response.text
    with open(f"/tmp/{SQL_FILE}", "w") as f:
        f.write(sql)
    print("✅ SQL file saved to /tmp/")
    return f"/tmp/{SQL_FILE}"

fetch_sql = PythonOperator(
    task_id="fetch_sql",
    python_callable=fetch_sql_from_github,
    dag=dag,
)

# ----------------------------
# Execute BigQuery Job
# ----------------------------
run_bq_sql = BigQueryInsertJobOperator(
    task_id="run_bq_sql",
    configuration={
        "query": {
            "query": open(f"/tmp/{SQL_FILE}").read() if os.path.exists(f"/tmp/{SQL_FILE}") else "",
            "useLegacySql": False,
            "defaultDataset": {"datasetId": DATASET_ID, "projectId": PROJECT_ID},
        }
    },
    location=LOCATION,
    dag=dag,
)

# ----------------------------
# DAG Task Order
# ----------------------------
fetch_sql >> run_bq_sql
