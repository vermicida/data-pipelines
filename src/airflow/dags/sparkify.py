from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator
)

# Loads the Sparkify configuration from the Airflow variables.
config = Variable.get('sparkify_config', deserialize_json=True)

# ---- #
# Dag #
# ---- #

dag = DAG(
    'sparkify',
    description='Sparkify data pipeline',
    schedule_interval='0 * * * *',
    catchup=False,
    default_args={
        'owner': 'udacity',
        'start_date': datetime(2019, 1, 12),
        'depends_on_past': False,
        'retries': config['dag']['retries'],
        'retry_delay': timedelta(minutes=config['dag']['retry_delay']),
        'email_on_retry': False
    }
)

# --------- #
# Operators #
# --------- #

start_operator = DummyOperator(
    task_id='Begin_execution',
    dag=dag
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    provide_context=True,
    redshift_conn_id='redshift',
    iam_role_arn=config['iam']['role_arn'],
    s3_prefix=config['s3']['log_data'],
    target_table=config['redshift']['staging_events_table'],
    json_path=config['s3']['log_data_json_path']
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    provide_context=True,
    redshift_conn_id='redshift',
    iam_role_arn=config['iam']['role_arn'],
    s3_prefix=config['s3']['song_data'],
    target_table=config['redshift']['staging_songs_table']
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    target_table=config['redshift']['songplays_table']
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    target_table=config['redshift']['users_table'],
    dimension=config['redshift']['users_table'],
    truncate=True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    target_table=config['redshift']['songs_table'],
    dimension=config['redshift']['songs_table'],
    truncate=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    target_table=config['redshift']['artists_table'],
    dimension=config['redshift']['artists_table'],
    truncate=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    target_table=config['redshift']['time_table'],
    dimension=config['redshift']['time_table'],
    truncate=True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    tables=(
        'songs',
        'artists',
        'users',
        'time',
        'songplays'
    )
)

end_operator = DummyOperator(
    task_id='Stop_execution',
    dag=dag
)

# ------------ #
# DAG workflow #
# ------------ #

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
