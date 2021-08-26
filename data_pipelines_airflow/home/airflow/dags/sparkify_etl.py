
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from helpers import SqlQueries
from operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator, CreateTableOperator)

default_args = {
    'owner': 'aoyerinde',
    'start_date': datetime(2020, 12, 16),
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'depends_on_past': False
}

dag = DAG(
    'etl_task',
    default_args=default_args,
    description='using airflow to etl in redshift',
    schedule_interval= '@hourly'
)

dummy_start_task  = DummyOperator(task_id='dummy_start_task',  dag=dag)



stage_events = StageToRedshiftOperator(
    task_id="stage_events",
    redshift_connection_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket='udacity-dend',
    s3_key="log_data/",
    extra_params="format as json 's3://udacity-dend/log_json_path.json'",
    dag=dag
)

stage_songs= StageToRedshiftOperator(
    task_id='stage_songs',
    redshift_connection_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket='udacity-dend',
    s3_key="song_data",
    extra_params="json 'auto' compupdate off region 'us-east-2'",
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id='load_songplays_table',
    redshift_connection_id="redshift",
    table="songplays",
    sql_query=SqlQueries.songplay_table_insert,
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='load_user_dimension_table',
    redshift_connection_id="redshift",
    table="users",
    insert_query=SqlQueries.user_table_insert,
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='load_song_dimension_table',
    redshift_connection_id="redshift",
    table="songs",
    insert_query=SqlQueries.song_table_insert,
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='load_artist_dimension_table',
    redshift_connection_id="redshift",
    table="artists",
    insert_query=SqlQueries.artist_table_insert,
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='load_time_dimension_table',
    redshift_connection_id="redshift",
    table="time",
    insert_query=SqlQueries.time_table_insert,
    dag=dag
)

quality_checks = DataQualityOperator(
    task_id='quality_checks',
    redshift_connection_id="redshift",
    table=['songplays', 'users','songs','artists','time'],
    min_expected_result=0,
    dag=dag
)


dummy_final_task = DummyOperator(task_id='dummy_final_task', trigger_rule = 'all_done', dag=dag)



dummy_start_task >> [stage_events, stage_songs] >>  load_songplays_table

load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> quality_checks >> dummy_final_task
