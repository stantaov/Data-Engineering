from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook


def create_table(**context):
    redhift_hook = PostgresHook('redshift')
    queries = open('/home/workspace/airflow/create_tables_capstone.sql', 'r').read()
    redhift_hook.run(queries)
    return

default_args = {
    'owner': 'Stan Taov'
}

# Create DAG with previously provided default_args
dag = DAG('dag_tables_capstone',
          default_args=default_args,
          description='Create tables in Redshift',
          start_date = datetime.now(),
          schedule_interval='@once'
        )

# This operator calls create_table function to create tables
create_tables_in_redshift = PythonOperator(
    task_id = 'Create_tables_in_redshift',
    dag = dag,
    provide_context = True,
    python_callable = create_table
)

