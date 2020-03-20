# Lint as: python3
"""Airflow DAG for generating schemaless and relational files"""
from datetime import timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from schemaless import create_schemaless
from schemaless import create_uuid_map
from relational import process_schemaless


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,  # TODO
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],  # TODO
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=15),
}


dag = DAG(
    'housing-dashboard-data',
    default_args=default_args,
    description=('Consume all available housing data and produce a unified '
                 'output.'),
    schedule_interval=timedelta(days=1),
)


task_create_schemaless = PythonOperator(
    task_id='create_schemaless',
    python_callable=create_schemaless.run,
    op_kwargs={
        'out_file': '{{ var.value.WORKDIR }}/schemaless.csv',
        'upload': '{{ var.value.UPLOAD }}',
        'diff': 'True',
    },
    dag=dag,
)

task_create_uuid_map = PythonOperator(
    task_id='create_uuid_map',
    python_callable=create_uuid_map.run,
    op_kwargs={
        'out_file': '{{ var.value.WORKDIR }}/uuid.csv',
        'likely_match_file': '{{ var.value.WORKDIR }}/likely-matches.csv',
        'upload': '{{ var.value.UPLOAD }}',
    },
    dag=dag,
)

task_create_relational = PythonOperator(
    task_id='create_relational',
    python_callable=process_schemaless.run,
    op_kwargs={
        'out_prefix': '{{ var.value.WORKDIR }}/',
        'upload': '{{ var.value.UPLOAD }}',
    },
    dag=dag,
)

task_create_schemaless >> task_create_uuid_map >> task_create_relational
