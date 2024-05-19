from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.operators.dummy import DummyOperator

# Constants
BUCKET_NAME = 'data_eng-hw_10-nazar_meliukh'
GCS_FOLDER = 'src1/sales/v1/{{ ds[:4] }}/{{ ds[5:7] }}/{{ ds[8:] }}/'
LOCAL_FILE_PATH = '/data/sales_{{ ds }}.csv'
DESTINATION_FILE_PATH = GCS_FOLDER + '{{ ds }}.csv'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'backfill': True,
}


with DAG(
        'gcs_upload',
        default_args=default_args,
        description='DAG to upload files to GCS',
        schedule_interval="0 1 * * *",
        max_active_runs=1,
        start_date=datetime(2022, 8, 9),
        catchup=True,
) as dag:
    start_task = DummyOperator(
        task_id='start'
    )

    # Task to upload a file to GCS
    upload_file_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_file_to_gcs',
        src=LOCAL_FILE_PATH,
        dst=DESTINATION_FILE_PATH,
        bucket=BUCKET_NAME,
        mime_type='text/plain'
    )

    end_task = DummyOperator(
        task_id='end'
    )

    start_task >> upload_file_to_gcs >> end_task
