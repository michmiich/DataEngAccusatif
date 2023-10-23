import airflow
import datetime
import pandas as pd
from faker import Faker
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from random import randint
import requests
from airflow.utils.task_group import TaskGroup

default_args_dict = {
    'start_date': airflow.utils.dates.days_ago(0),
    'concurrency': 1,
    'schedule_interval': None,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
}

global_dag = DAG(
    dag_id='global_dag',
    default_args=default_args_dict,
    catchup=False,
    template_searchpath=['/opt/airflow/dags/']
)

with TaskGroup("ingestion_pipeline","data ingestion step",dag=global_dag) as ingestion_pipeline:
    ingestion_start = DummyOperator(
        task_id='start',
        dag=global_dag,
    )

    ingestion_end = DummyOperator(
        task_id='end',
        dag=global_dag,
        trigger_rule='all_success'
    )

    ingestion_start >> ingestion_end

with TaskGroup("staging_pipeline","data staging step",dag=global_dag) as staging_pipeline:
    stagin_start = DummyOperator(
        task_id='start',
        dag=global_dag,
    )

    staging_end = DummyOperator(
        task_id='staging_end',
        dag=global_dag,
        trigger_rule='all_success'
    )

    stagin_start >> staging_end

start_global = DummyOperator(
    task_id='start_global',
    dag=global_dag,
    trigger_rule='all_success'
)

end_global = DummyOperator(
    task_id='end_global',
    dag=global_dag,
    trigger_rule='all_success'
)

start_global >> ingestion_pipeline >> staging_pipeline >> end_global