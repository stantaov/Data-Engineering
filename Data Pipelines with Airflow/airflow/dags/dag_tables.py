from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import PythonOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from airflow.models import Variable
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from helpers import SqlQueries

# Create function that creates all tables in Redshift
def create_table(**context):
    #aws_hook = AwsHook('aws_credentials')
    #credentials = aws_hook.get_credentials()
    redhift_hook = PostgresHook('redshift')
    queries = open('/home/workspace/airflow/create_tables.sql', 'r').read()
    redhift_hook.run(queries)
    return

default_args = {
    'owner': 'Stan Taov',
    'retries': 3,
    'retry_delay': timedelta(seconds = 300)
}

# Create DAG with previously provided default_args
dag = DAG('dag_tables',
          default_args=default_args,
          description='Create tables in Redshift',
          start_date = datetime.now()
        )

# This operator calls create_table function to create tables
create_tables_in_redshift = PythonOperator(
    task_id = 'Create_tables_in_redshift',
    dag = dag,
    provide_context = True,
    python_callable = create_table
)

