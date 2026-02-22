
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyTableOperator,
    BigQueryDeleteDatasetOperator,
)
from airflow.providers.google.cloud.operators.bigquery import GCSToBigQueryOperator

# Define default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 2, 17),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define your GCP project, bucket, dataset, and table details
PROJECT_ID = "myfirstcloudproject-487412"
BUCKET_NAME = "rawdatatoprocess"
SOURCE_OBJECTS = ["employee.csv"] # Can use wildcards like ['folder/*.csv']
DATASET_NAME = "rawdataprocess"
TABLE_NAME = "employee"
DESTINATION_TABLE = f"{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}"

# Instantiate the DAG
with DAG(
    'gcs_to_bigquery_load',
    default_args=default_args,
    description='Load data from GCS to BigQuery using the operator',
    schedule=timedelta(days=1),
    catchup=False,
) as dag:
    # Task to create the BigQuery table if it does not exist (optional but good practice)
    # create_table = BigQueryCreateTableOperator(
    #    task_id="start_Job"
    #    project_id='myfirstcloudproject-487412',
    #    dataset_id='rawdataprocess',
    #    table_id='employee',
    #    table_resource=None,
    #    gcp_conn_id='google_cloud_default' # Connection ID configured in Airflow
   # )

    start_task = EmptyOperator(
        task_id='start',
        dag=dag,
    )

    # Task to load data from GCS to BigQuery
    load_data = GCSToBigQueryOperator(
        task_id="load_data",
        bucket="rawdatatoprocess", #gs://rawdatatoprocess/Employee.csv
        source_objects=["employee.csv"],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}",
        source_format="csv", # Specify the source file format (CSV, NEWLINE_DELIMITED_JSON, etc.)
        skip_leading_rows=1, # Skip the header row if present in CSV
        write_disposition="WRITE_TRUNCATE", # Overwrites the table if it already exists
        external_table=False,
        gcp_conn_id="google_cloud_default",
        schema_fields=[
           {"name": "Emp_ID", "type": "INTEGER", "mode": "REQUIRED"},
           {"name": "First_Name", "type": "STRING", "mode": "NULLABLE"},
           {"name": "Surname", "type": "STRING", "mode": "NULLABLE"},
           {"name": "Department", "type": "STRING", "mode": "NULLABLE"},
           {"name": "Salary", "type": "FLOAT", "mode": "NULLABLE"},
           {"name": "ZIP_Code", "type": "FLOAT", "mode": "NULLABLE"},
           {"name": "City", "type": "STRING", "mode": "NULLABLE"},
           {"name": "Country", "type": "STRING", "mode": "NULLABLE"},
           {"name": "Manager", "type": "STRING", "mode": "NULLABLE"},
        ]
    )

    # Define task dependencies
    start_task >> load_data
