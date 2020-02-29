from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

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

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    aws_credentials_id="aws_credentials",
    dag=dag,
    json_path="s3://udacity-dend/log_json_path.json",
    redshift_conn_id="redshift",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    table="staging_events",
    task_id="load_events_from_s3_to_redshift",
)

stage_songs_to_redshift = StageToRedshiftOperator(
    aws_credentials_id="aws_credentials",
    dag=dag,
    json_path="auto",
    redshift_conn_id="redshift",
    s3_bucket="udacity-dend",
    s3_key="song_data/A/A/A",
    table="staging_songs",
    task_id="load_songs_from_s3_to_redshift",
)

load_songplays_table = LoadFactOperator(
    dag=dag,
    redshift_conn_id="redshift",
    sql_stmt=SqlQueries.songplay_table_insert,
    table='songplays',
    task_id='Load_songplays_fact_table',
)

load_user_dimension_table = LoadDimensionOperator(
    dag=dag,
    redshift_conn_id="redshift",
    sql_stmt=SqlQueries.user_table_insert,
    table='users',
    task_id='Load_user_dim_table',
)

load_song_dimension_table = LoadDimensionOperator(
    dag=dag,
    redshift_conn_id="redshift",
    sql_stmt=SqlQueries.song_table_insert,
    table='songs',
    task_id='Load_song_dim_table',
)

load_artist_dimension_table = LoadDimensionOperator(
    dag=dag,
    redshift_conn_id="redshift",
    sql_stmt=SqlQueries.artist_table_insert,
    table='artists',
    task_id='Load_artist_dim_table',
)

load_time_dimension_table = LoadDimensionOperator(
    dag=dag,
    redshift_conn_id="redshift",
    sql_stmt=SqlQueries.songplay_table_insert,
    table='songplays',
    task_id='Load_time_dim_table',
)

run_quality_checks = DataQualityOperator(
    dag=dag,
    redshift_conn_id="redshift",
    tables=['artists', 'songplays', 'songs', 'time', 'users'],
    task_id='Run_data_quality_checks',
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)
