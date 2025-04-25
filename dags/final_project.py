from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

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

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id='redshift_default',
        aws_credentials_id='aws_credentials',
        table='staging_events',
        s3_bucket='wgu-udacity-d608-bjordan',
        s3_key='log-data'
        json_path='s3://wgu-udacity-d608-bjordan/log_json_path.json',
        provide_context=True
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id='redshift_default',
        aws_credentials_id='aws_credentials',
        table='staging_songs',
        s3_bucket='wgu-udacity-d608-bjordan',
        s3_key='song-data'
        json_path='auto',
        provide_context=True
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

final_project_dag = final_project()
