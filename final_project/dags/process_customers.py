"""
Customers processing pipeline
"""
import datetime as dt

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, \
    BigQueryInsertJobOperator, BigQueryDeleteTableOperator

DATA_LAKE_RAW_BUCKET = 'data_eng-hw_final_project_raw_data_nazar_meliukh'
DATASET_NAME = 'de-07-nazar-meliukh'
BUCKET_FILE_NAME = 'customers/{{ dag_run.logical_date.strftime("%Y-%m-%-d") }}/*.csv'
DEFAULT_ARGS = {
    'depends_on_past': True,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': 5,
}

with DAG(
        dag_id="customers_pipeline",
        description="Ingest and process customers data",
        schedule_interval='0 7 * * *',
        start_date=dt.datetime(2022, 8, 1),
        end_date=dt.datetime(2022, 8, 6),
        catchup=True,
        tags=['customers'],
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
) as dag:

    delete_bronze_table_task = BigQueryDeleteTableOperator(
        task_id="delete_bronze_table",
        deletion_dataset_table=f'{DATASET_NAME}.bronze.customers',
        ignore_if_missing=True,
    )

    create_external_table_task = BigQueryCreateExternalTableOperator(
        task_id='create_external_table',
        destination_project_dataset_table=f'{DATASET_NAME}.bronze.customers',
        bucket=DATA_LAKE_RAW_BUCKET,
        source_objects=[BUCKET_FILE_NAME],
        schema_fields=[
            {"name": "Id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "FirstName", "type": "STRING", "mode": "NULLABLE"},
            {"name": "LastName", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Email", "type": "STRING", "mode": "NULLABLE"},
            {"name": "RegistrationDate", "type": "STRING", "mode": "NULLABLE"},
            {"name": "State", "type": "STRING", "mode": "NULLABLE"},
        ],
        skip_leading_rows=1,
    )


    transfer_from_dwh_bronze_to_dwh_silver = BigQueryInsertJobOperator(
        task_id='transfer_from_dwh_bronze_to_dwh_silver',
        location='US',
        project_id=DATASET_NAME,
        configuration={
            "query": {
                "query": "{% include 'sql/customers_dwh_bronze_to_dwh_silver.sql' %}",
                "useLegacySql": False,
            }
        },
        params={
            'project_id': DATASET_NAME
        }
    )

    delete_bronze_table_task >> create_external_table_task >> transfer_from_dwh_bronze_to_dwh_silver
