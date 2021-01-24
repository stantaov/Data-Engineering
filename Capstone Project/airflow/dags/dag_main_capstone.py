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


# Create default_args object with the following keys:
# Owner, Depends_on_past, Start_date, Retries, Retry_delay, Catchup.
default_args = {
    'owner': 'Stan Taov',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 12),
    'retries': 3,
    'retry_delay': timedelta(seconds = 300),
    'catchup': False
}

# Create DAG with previously provided default_args
# DAG will be running once
dag = DAG('dag_main_capstone',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@once'
        )

# Create a dummy start operator to identify the beginning of the DAG
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# Load data from S3 to i94 table on Redshift
stage_i94_to_redshift = StageToRedshiftOperator(
    task_id='Stage_i94',
    dag=dag,
    table = 'i94',
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    s3_bucket = "s3://udacity-data-engineering-stan/data/i94/"
)

# Load data from S3 to staging_songs table on Redshift
stage_temperature_to_redshift = StageToRedshiftOperator(
    task_id='Stage_temperatures',
    dag=dag,
    table = 'temperatures',
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    s3_bucket = "s3://udacity-data-engineering-stan/data/temperatures/"
)

# Load data from S3 to staging_songs table on Redshift
stage_airports_code_to_redshift = StageToRedshiftOperator(
    task_id='Stage_airports_code',
    dag=dag,
    table = 'airports_code',
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    s3_bucket = "s3://udacity-data-engineering-stan/data/airports_code/"
)

stage_visas_code_to_redshift = StageToRedshiftOperator(
    task_id='Stage_visas_code',
    dag=dag,
    table = 'visas_code',
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    s3_bucket = "s3://udacity-data-engineering-stan/data/visas_code/"
)

stage_modes_code_to_redshift = StageToRedshiftOperator(
    task_id='Stage_modes_code',
    dag=dag,
    table = 'modes_code',
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    s3_bucket = "s3://udacity-data-engineering-stan/data/modes_code/"
)

stage_countries_code_to_redshift = StageToRedshiftOperator(
    task_id='Stage_countries_code',
    dag=dag,
    table = 'countries_code',
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    s3_bucket = "s3://udacity-data-engineering-stan/data/countries_code/"
)

stage_states_code_to_redshift = StageToRedshiftOperator(
    task_id='Stage_states_code',
    dag=dag,
    table = 'states_code',
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    s3_bucket = "s3://udacity-data-engineering-stan/data/states_code/"
)

stage_demographics_to_redshift = StageToRedshiftOperator(
    task_id='Stage_demographics',
    dag=dag,
    table = 'demographics',
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    s3_bucket = "s3://udacity-data-engineering-stan/data/demographics/"
)

stage_airports_data_to_redshift = StageToRedshiftOperator(
    task_id='Stage_airports',
    dag=dag,
    table = 'airports_data',
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    s3_bucket = "s3://udacity-data-engineering-stan/data/airports_data/"
)


# Load data from staging tables to fact songplays table
load_fact_table = LoadFactOperator(
    task_id='Load_data_to_fact_table',
    dag=dag,
    table = 'fact',
    redshift_conn_id = 'redshift',
    sql_statement = SqlQueries.fact_table_insert,
    append_data = False
)

load_visitors_dimension_table = LoadDimensionOperator(
    task_id='Load_visitors_dim_table',
    dag=dag,
    table = 'visitors',
    redshift_conn_id = 'redshift',
    sql_statement = SqlQueries.visitors_table_insert,
    append_data = False
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table = 'time',
    redshift_conn_id = 'redshift',
    sql_statement = SqlQueries.time_table_insert,
    append_data = False
)


load_visit_dimension_table = LoadDimensionOperator(
    task_id='Load_visit_dim_table',
    dag=dag,
    table = 'visit',
    redshift_conn_id = 'redshift',
    sql_statement = SqlQueries.visit_table_insert,
    append_data = False
)

load_visa_dimension_table = LoadDimensionOperator(
    task_id='Load_visa_dim_table',
    dag=dag,
    table = 'visa',
    redshift_conn_id = 'redshift',
    sql_statement = SqlQueries.visa_table_insert,
    append_data = False
)

load_flag_dimension_table = LoadDimensionOperator(
    task_id='Load_flag_dim_table',
    dag=dag,
    table = 'flag',
    redshift_conn_id = 'redshift',
    sql_statement = SqlQueries.flag_table_insert,
    append_data = False
)

# Check data quality
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = 'redshift',
    tables = ["public.i94", "public.temperatures", "public.airports_code", "public.visas_code", "public.modes_code", "public.countries_code", "public.states_code", "public.demographics", "public.airports_data", "public.fact", "public.visitors", "public.time", "public.visit", "public.visa", "public.flag"]
)

# Create a dummy end operator to identify the end of the DAG
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)



start_operator >> [stage_i94_to_redshift, stage_temperature_to_redshift, stage_airports_code_to_redshift, stage_visas_code_to_redshift, stage_modes_code_to_redshift, stage_countries_code_to_redshift, stage_states_code_to_redshift, stage_demographics_to_redshift, stage_airports_data_to_redshift]
[stage_i94_to_redshift, stage_temperature_to_redshift, stage_airports_code_to_redshift, stage_visas_code_to_redshift, stage_modes_code_to_redshift, stage_countries_code_to_redshift, stage_states_code_to_redshift, stage_demographics_to_redshift, stage_airports_data_to_redshift] >> load_fact_table
load_fact_table >> [load_visitors_dimension_table, load_time_dimension_table, load_visit_dimension_table, load_visa_dimension_table, load_flag_dimension_table]
[load_visitors_dimension_table, load_time_dimension_table, load_visit_dimension_table, load_visa_dimension_table, load_flag_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator


