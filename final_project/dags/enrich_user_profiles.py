"""
enrich_user_profiles pipeline
"""
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


DATASET_NAME = 'de-07-nazar-meliukh'

DEFAULT_ARGS = {
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': 5,
}

with DAG(
        dag_id="enrich_user_profiles_pipeline",
        description="Enrich user profiles data",
        schedule_interval=None,
        tags=['enrich_user_profiles'],
        default_args=DEFAULT_ARGS,
) as dag:

    enrich_user_profiles_task = BigQueryInsertJobOperator(
        task_id='enrich_user_profiles',
        location='US',
        project_id=DATASET_NAME,
        configuration={
            "query": {
                "query": "{% include 'sql/enrich_user_profiles.sql' %}",
                "useLegacySql": False,
            }
        },
        params={
            'project_id': DATASET_NAME
        }
    )

    enrich_user_profiles_task
