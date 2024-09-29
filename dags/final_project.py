from datetime import datetime, timedelta
import pendulum
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.secrets.metastore import MetastoreBackend
from airflow.hooks.postgres_hook import PostgresHook
from operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    "owner": 'hieulm',
    "start_date": datetime(2024, 9, 28),
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
    "email_on_retry": False,
}

def create_tables():
    metastoreBackend = MetastoreBackend()
    aws_connection=metastoreBackend.get_connection('aws_credentials')
    print('key', aws_connection.password)
    redshift_hook = PostgresHook('redshift_connection')
    redshift_hook.run(SqlQueries.create_tables)
# set DAG to hourly according to project rubric
dag = DAG('s3_to_redshift',
          default_args=default_args,
          schedule_interval='@daily'
        )
start_operator = DummyOperator(task_id='begin_execution',dag=dag)

create_tables = PythonOperator(
    task_id='create_tables',
    dag=dag,
    python_callable= create_tables
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='stage_events',
    dag=dag,
    aws_credentials_id = 'aws_credentials',
    redshift_conn_id = 'redshift_connection',
    table="staging_events",
    s3_bucket = 'hieulm',
    s3_key = 'log-data',
    region='us-east-1',
    json_format="s3://hieulm/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='stage_songs',
    dag=dag,
    aws_credentials_id = 'aws_credentials',
    redshift_conn_id = 'redshift_connection',
    table="staging_songs",
    s3_bucket = 'hieulm',
    s3_key = 'song-data/A/A/A',
    region='us-east-1',
    json_format="auto"
)

load_songplays_table = LoadFactOperator(
    task_id='load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift_connection",
    sql=SqlQueries.songplay_table_insert,
    table='songplays',
    truncate=True,
)


load_user_dimension_table = LoadDimensionOperator(
    task_id='load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift_connection",
    table="users",
    sql=SqlQueries.user_table_insert,
    truncate=True,
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift_connection",
    table="songs",
    sql=SqlQueries.song_table_insert,
    truncate=True,
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift_connection",
    table="artists",
    sql=SqlQueries.artist_table_insert,
    truncate=True,
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift_connection",
    table="time",
    sql=SqlQueries.time_table_insert,
    truncate=True,
)

run_quality_checks = DataQualityOperator(
    task_id='run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift_connection",
    condition_check=[
        {
            "table": "users",
            "column": "userid",
        },
    ]
)
end_operator = DummyOperator(task_id='end_execution',dag=dag)

start_operator >> create_tables >> \
    [stage_events_to_redshift, stage_songs_to_redshift] >> \
        load_songplays_table >> \
            [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> \
            run_quality_checks >> end_operator