from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator, CreateTableOperator)
from airflow.operators.dummy_operator import DummyOperator

from helpers import SqlQueries

s3_bucket = 'udacity-dend'
song_s3_key = "song_data"
log_s3_key = "log_data"
log_json_file = "log_json_path.json"

default_args = {
    'owner': 'udacity',
    'depends_on_past': True,
    'start_date': datetime(2021, 1, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': True
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
          )

create_table = CreateTableOperator(task_id="Create_table", dag=dag, conn_id="redshift")

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table_name="staging_events",
    s3_bucket=s3_bucket,
    s3_key=log_s3_key,
    log_json_file=log_json_file,
    conn_id="redshift",
    aws_credential_id="aws_credentials",
    provide_context=True
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table_name="staging_songs",
    s3_bucket=s3_bucket,
    s3_key=song_s3_key,
    conn_id="redshift",
    aws_credential_id="aws_credentials",
    provide_context=True
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    conn_id="redshift",
    table_name="songplays",
    sql_query=SqlQueries.songplay_table_insert,
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    conn_id="redshift",
    sql_query=SqlQueries.user_table_insert,
    empty_table=True,
    table_name="users",
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    conn_id="redshift",
    sql_query=SqlQueries.song_table_insert,
    empty_table=True,
    table_name="songs",
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    conn_id="redshift",
    sql_query=SqlQueries.artist_table_insert,
    empty_table=True,
    table_name="artists",
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    conn_id="redshift",
    sql_query=SqlQueries.time_table_insert,
    empty_table=True,
    table_name="time",
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    conn_id="redshift",
    tables={
        "users": 104,
        "songplays": 6820,
        "time": 6820,
        "artists": 10025,
        "songs": 14896,
        "staging_events": 8056,
        "staging_songs": 14896,
    }
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> create_table

create_table >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

load_songplays_table >> [load_song_dimension_table, load_artist_dimension_table,
                         load_time_dimension_table, load_user_dimension_table] >> run_quality_checks >> end_operator
