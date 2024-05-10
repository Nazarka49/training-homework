from datetime import datetime
import os

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.models import Variable


BASE_DIR = Variable.get('BASE_DIR')


def run_job1(**kwargs):
    execution_date = kwargs['execution_date'].strftime("%Y-%m-%d")
    raw_dir = os.path.join(BASE_DIR, "raw", "sales", execution_date)
    http_hook = HttpHook(
        method='POST',
        http_conn_id='job_1'
    )
    json_date = {
        "date": execution_date,
        "raw_dir": raw_dir
    }
    print(json_date)
    response = http_hook.run('',json=json_date)
    assert response.status_code == 201


def run_job2(**kwargs):
    execution_date = kwargs['execution_date'].strftime("%Y-%m-%d")
    raw_dir=os.path.join(BASE_DIR, "raw", "sales", execution_date)
    stg_dir=os.path.join(BASE_DIR, "stg", "sales", execution_date)
    http_hook = HttpHook(
        method='POST',
        http_conn_id='job_2'
    )
    json = {
        "raw_dir": raw_dir,
        "stg_dir": stg_dir
    }
    response = http_hook.run(json=json)
    assert response.status_code == 201


DEFAULT_ARGS = {
    'depends_on_past': False,
    'email': ['nazarka49@gmail.com'],
    'email_on_failure': True,
}

with DAG(
        dag_id='process_sales',
        start_date=datetime(2022, 8, 9),
        end_date=datetime(2022, 8, 12),
        schedule_interval="0 1 * * *",
        max_active_runs=1,
        catchup=True,
        default_args=DEFAULT_ARGS,
) as dag:
    start = EmptyOperator(
        task_id='start'
    )

    extract_data_from_api_task = PythonOperator(
        task_id='extract_data_from_api',
        python_callable=run_job1,
        provide_context=True
    )

    convert_to_avro_task = PythonOperator(
        task_id='convert_to_avro',
        python_callable=run_job2,
        provide_context=True
    )

    finish = EmptyOperator(
        task_id='finish'
    )

    start >> extract_data_from_api_task >> convert_to_avro_task >> finish
