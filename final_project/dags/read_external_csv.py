"""
Sales processing pipeline
"""
import datetime as dt

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, \
    BigQueryInsertJobOperator, BigQueryDeleteTableOperator


DATA_LAKE_RAW_BUCKET = 'data_eng-hw_final_project_raw_data_nazar_meliukh'
DATASET_NAME = 'de-07-nazar-meliukh'
BUCKET_FILE_NAME = 'sales/{{ dag_run.logical_date.strftime("%Y-%m-%-d") }}/{{ dag_run.logical_date.strftime("%Y-%m-%-d") }}__sales.csv'

DEFAULT_ARGS = {
    'depends_on_past': True,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': 5,
}

with DAG(
        dag_id="sales_pipeline",
        description="Ingest and process sales data",
        schedule_interval='0 7 * * *',
        start_date=dt.datetime(2022, 9, 1),
        end_date=dt.datetime(2022, 9, 7),
        catchup=True,
        tags=['sales'],
        default_args=DEFAULT_ARGS,
) as dag:

    create_external_table_task = BigQueryCreateExternalTableOperator(
        task_id='create_external_table',
        destination_project_dataset_table=f'{DATASET_NAME}.bronze.sales',
        bucket=DATA_LAKE_RAW_BUCKET,
        source_objects=[BUCKET_FILE_NAME],
        schema_fields=[
            {"name": "CustomerId", "type": "STRING", "mode": "NULLABLE"},
            {"name": "PurchaseDate", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Product", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Price", "type": "STRING", "mode": "NULLABLE"},
        ],
        skip_leading_rows=1,
    )

    transfer_from_dwh_bronze_to_dwh_silver = BigQueryInsertJobOperator(
        task_id='transfer_from_dwh_bronze_to_dwh_silver',
        location='US',
        project_id=DATASET_NAME,
        configuration={
            "query": {
                "query": "{% include 'sql/transfer_from_dwh_bronze_to_dwh_silver.sql' %}",
                "useLegacySql": False,
            }
        },
        params={
            'project_id': DATASET_NAME
        }
    )

    delete_bronze_table_task = BigQueryDeleteTableOperator(
        task_id="delete_bronze_table",
        deletion_dataset_table=f'{DATASET_NAME}.bronze.sales',
    )
    create_external_table_task >> transfer_from_dwh_bronze_to_dwh_silver >> delete_bronze_table_task
