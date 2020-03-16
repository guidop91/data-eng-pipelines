from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries


default_args = {
    'owner': 'guido',
    'start_date': datetime(2019, 1, 12),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'depends_on_past': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup=False
          )

# Mark start of pipeline
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# Populate events table with origin data from S3
stage_events_to_redshift = StageToRedshiftOperator(
    task_id="load_events_from_s3_to_redshift",
    aws_credentials_id="aws_credentials",
    dag=dag,
    redshift_conn_id="redshift",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    table="staging_events",
    json_path='s3://udacity-dend/log_json_path.json'
)

# Populate songs table with origin data from S3
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id="load_songs_from_s3_to_redshift",
    aws_credentials_id="aws_credentials",
    dag=dag,
    redshift_conn_id="redshift",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    ignore_headers="0",
    table="staging_songs",
)

# Extract data for songplays table and insert
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_query=SqlQueries.songplay_table_insert,
    table='songplays',
)

# Extract data for users table and insert
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_query=SqlQueries.user_table_insert,
    table='users',
)

# Extract data for songs table and insert
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_query=SqlQueries.song_table_insert,
    table='songs',
)

# Extract data for artists table and insert
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_query=SqlQueries.artist_table_insert,
    table='artists',
)

# Extract data for time table and insert
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_query=SqlQueries.time_table_insert,
    table='time',
)

# Make sure tables contain at least one record
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=['artists', 'songplays', 'songs', 'time', 'users'],
)

# Mark end of pipeline
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Dag orchestrator
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
