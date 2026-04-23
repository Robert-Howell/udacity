from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'start_date': pendulum.now(),
    'catchup': False,
    'email_on_retry': False
}
s3 = Variable.get('s3_bucket')

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly'
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id='redshift_default',
        aws_credentials_id='aws_credentials',
        table='staging_events',
        s3_bucket=s3,
        s3_key='log-data',
        s3_path=f's3://{s3}/log-data',
        json_path=f's3://{s3}/log_json_path.json'
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id='redshift_default',
        aws_credentials_id='aws_credentials',
        table='staging_songs',
        s3_bucket=s3,
        s3_key='song-data',
        s3_path=f's3://{s3}/song-data',
        json_path='auto'
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id='redshift_default',
        table='songplays',
        sql_statement=SqlQueries.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id='redshift_default',
        table='users',
        sql_statement=SqlQueries.user_table_insert
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id='redshift_default',
        table='songs',
        sql_statement=SqlQueries.song_table_insert
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id='redshift_default',
        table='artists',
        sql_statement=SqlQueries.artist_table_insert
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id='redshift_default',
        table='time',
        sql_statement=SqlQueries.time_table_insert
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id='redshift_default',
        test_cases=[
            {'check_sql': 'SELECT COUNT(*) FROM songplays WHERE playid IS NULL;', 'expected_result': 0},
            {'check_sql': 'SELECT COUNT(*) FROM users WHERE userid IS NULL;', 'expected_result': 0}
        ]
    )

    end_operator = DummyOperator(task_id='Stop_execution')

    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks >> end_operator

final_project_dag = final_project()
