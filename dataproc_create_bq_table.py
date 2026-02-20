import sys
from pyspark.sql import SparkSession
from google.cloud import bigquery

# Set your GCP project and dataset/table details
GCP_PROJECT = 'your-gcp-project-id'
DATASET_ID = 'your_dataset'
TABLE_ID = 'your_table'

# Initialize Spark session
spark = SparkSession.builder \
    .appName('Dataproc PySpark BigQuery Table Creator') \
    .getOrCreate()

# Initialize BigQuery client
bq_client = bigquery.Client(project=GCP_PROJECT)

def create_bq_table():
    schema = [
        bigquery.SchemaField('id', 'INTEGER', mode='REQUIRED'),
        bigquery.SchemaField('name', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('created_at', 'TIMESTAMP', mode='NULLABLE'),
    ]
    table_ref = f"{GCP_PROJECT}.{DATASET_ID}.{TABLE_ID}"
    table = bigquery.Table(table_ref, schema=schema)
    try:
        table = bq_client.create_table(table)
        print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")
    except Exception as e:
        print(f"Error creating table: {e}")

def main():
    create_bq_table()
    spark.stop()

if __name__ == "__main__":
    main()

   # test_table_ref = f"{GCP_PROJECT}.{DATASET_ID}.{TABLE_ID}"
