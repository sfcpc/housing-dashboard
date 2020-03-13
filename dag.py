# Lint as: python3
"""Airflow DAG for generating schemaless and relational files"""
from datetime import timedelta

from airflow import DAG
# from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago


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

task = BashOperator(
    task_id='create_schemaless',
    bash_command=('python -m create_schemaless '
                  '--parcel_data_file={{ var.value.WORKDIR }}/../data/assessor/2020-02-18-parcels.csv.xz '  # NOQA
                  '--planning_file {{ var.value.WORKDIR }}/../testdata/planning-two.csv '  # NOQA
                  '--no_download True '
                  '{{ var.value.WORKDIR }}/schemaless.csv'),
    dag=dag,
)
