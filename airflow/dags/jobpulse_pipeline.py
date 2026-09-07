import sys
from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

# Add JobPulse source directories to Python's module search path
sys.path.insert(0, "/opt/airflow/src")
sys.path.insert(0, "/opt/airflow/src/ingestion/startup_jobs")

from ingestion.startup_jobs.ingest import run_ingestion


with DAG(
    dag_id="jobpulse_pipeline",
    start_date=datetime(2026, 9, 1),
    schedule="@daily",
    catchup=False,
    tags=["jobpulse", "ingestion", "databricks"],
) as dag:

    ingest_jobs = PythonOperator(
        task_id="ingest_jobs",
        python_callable=run_ingestion,
    )

    run_databricks_pipeline = DatabricksSubmitRunOperator(
        task_id="run_databricks_pipeline",
        databricks_conn_id="databricks_default",
        pipeline_task={
            "pipeline_id": "1bce996d-44a8-4215-b7b6-5b83ece33bbb"
        },
    )

    ingest_jobs >> run_databricks_pipeline