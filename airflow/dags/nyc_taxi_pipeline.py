from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.utils.dates import days_ago

PROJECT_ID = "gcp-data-project-478906"
REGION = "us-east1"
CLUSTER_NAME = "my-nyc-dataset-cluster"
BUCKET = "nyc-tlc-yellow-2024"

default_args = {
    "owner": "aulon",
    "start_date": days_ago(1),
}

with DAG(
    dag_id="nyc_taxi_etl_dataproc",
    default_args=default_args,
    description="ETL pipeline for NYC Taxi data using Dataproc and Spark",
    schedule_interval=None,
    catchup=False,
    tags=["dataproc", "nyc-taxi", "etl"],
) as dag:

    extract_job = {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
           "pyspark_job": {"main_python_file_uri": f"gs://{BUCKET}/spark-jobs/data_extract.py"},
    }

    transform_job = {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
           "pyspark_job": {"main_python_file_uri": f"gs://{BUCKET}/spark-jobs/data_transform.py"},
    }

    load_job = {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
           "pyspark_job": {"main_python_file_uri": f"gs://{BUCKET}/spark-jobs/data_load.py"},
    }

    extract = DataprocSubmitJobOperator(
        task_id="extract",
        job=extract_job,
        region=REGION,
        project_id=PROJECT_ID,
        gcp_conn_id="google_cloud_default",
    )

    transform = DataprocSubmitJobOperator(
        task_id="transform",
        job=transform_job,
        region=REGION,
        project_id=PROJECT_ID,
        gcp_conn_id="google_cloud_default",
    )

    load = DataprocSubmitJobOperator(
        task_id="load",
        job=load_job,
        region=REGION,
        project_id=PROJECT_ID,
        gcp_conn_id="google_cloud_default",
    )

    extract >> transform >> load
