"""
User profiles processing pipeline
"""
import datetime as dt

from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

DATA_LAKE_RAW_BUCKET = 'data_eng-hw_final_project_raw_data_nazar_meliukh'
DATASET_NAME = 'de-07-nazar-meliukh'
BUCKET_FILE_NAME = 'user_profiles/user_profiles.json'

DEFAULT_ARGS = {
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': 5,
}

with DAG(
        dag_id="user_profiles_pipeline",
        description="Ingest and process user profiles data",
        schedule_interval=None,
        tags=['user_profiles'],
        default_args=DEFAULT_ARGS,
) as dag:

    profiles_to_dwh_silver_task = GCSToBigQueryOperator(
        task_id='transfer_user_profiles_to_dwh_silver',
        bucket=DATA_LAKE_RAW_BUCKET,
        source_objects=[BUCKET_FILE_NAME],
        destination_project_dataset_table=f'{DATASET_NAME}.silver.user_profiles',
        schema_fields=[
            {'name': 'email', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'full_name', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'state', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'birth_date', 'type': 'DATE', 'mode': 'REQUIRED'},
            {'name': 'phone_number', 'type': 'STRING', 'mode': 'REQUIRED'},
        ],
        source_format='NEWLINE_DELIMITED_JSON',
        write_disposition='WRITE_TRUNCATE'
    )

    profiles_to_dwh_silver_task
