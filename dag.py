# Lint as: python3
"""Airflow DAG for generating schemaless and relational files"""
from datetime import timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from schemaless.create_schemaless import run


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
    python_callable=run,
    op_kwargs={
        'out_file': '{{ var.value.WORKDIR }}/schemaless.csv',
        'no_download': 'True',
        'planning_file': '{{ var.value.WORKDIR }}/../testdata/planning-two.csv',  # NOQA
        'parcel_data_file': '{{ var.value.WORKDIR }}/../data/assessor/2020-02-18-parcels.csv.xz',  # NOQA
    },
    dag=dag,
)
