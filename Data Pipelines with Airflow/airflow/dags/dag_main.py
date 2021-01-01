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
# DAG will be running hourly
dag = DAG('dag_main',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly'
        )

# Create a dummy start operator to identify the beginning of the DAG
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# Load data from S3 to staging_events table on Redshift
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table = 'staging_events',
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    s3_bucket = Variable.get('s3_bucket'),
    s3_prefix = "log_data",
    json_path = "s3://udacity-dend/log_json_path.json"
    
)

# Load data from S3 to staging_songs table on Redshift
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table = 'staging_songs',
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    s3_bucket = Variable.get('s3_bucket'),
    s3_prefix = "song_data/A/A/A/"
)

# Load data from staging tables to fact songplays table
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table = 'songplays',
    redshift_conn_id = 'redshift',
    sql_statement = SqlQueries.songplay_table_insert,
    append_data = True
)

# Load data from staging tables to dimension users table
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table = 'users',
    redshift_conn_id = 'redshift',
    sql_statement = SqlQueries.user_table_insert,
    append_data = True
)

# Load data from staging tables to dimension users table
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table = 'songs',
    redshift_conn_id = 'redshift',
    sql_statement = SqlQueries.song_table_insert,
    append_data = True
)

# Load data from staging tables to dimension artists table
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table = 'artists',
    redshift_conn_id = 'redshift',
    sql_statement = SqlQueries.artist_table_insert,
    append_data = True
)

# Load data from staging tables to dimension time table
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table = 'time',
    redshift_conn_id = 'redshift',
    sql_statement = SqlQueries.time_table_insert,
    append_data = True
)

# Check data quality
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = 'redshift',
    tables = ["public.songplays", "public.users", "public.songs", "public.artists", "public.time"]
)

# Create a dummy end operator to identify the end of the DAG
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)



start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table]
[load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator



