from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

start_date = datetime.utcnow() - timedelta(days=+31)

default_args = {
    'owner': 'sumer',
    'start_date': datetime(2021, 1, 1),  
    'end_date': datetime(2021, 1, 31),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('yelp_dag1',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@monthly',   #0 * * * *'    # hourly
          max_active_runs=1              # only testing          
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# Table creation code if not creating directly in Redshift 
create_tables_task = PostgresOperator(
  task_id="create_tables",
  dag=dag,
  sql='create_tables.sql', 
  postgres_conn_id="redshift"
)

stage_reviews_to_redshift = StageToRedshiftOperator(
    task_id='Stage_reviews',
    dag=dag,
    provide_context=True,
    table="staging_reviews",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="yelp-data-small",
    s3_key="reviews_df",
    region="ap-southeast-1",
    file_format="JSON",
    execution_date=start_date  
)

stage_restaurants_to_redshift = StageToRedshiftOperator(
    task_id='Stage_restaurants',
    dag=dag,
    provide_context=True,
    table="staging_restaurants",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="yelp-data_small",
    s3_key="restaurants_df",
    region="ap-southeast-1",
    data_format="CSV",
    execution_date=start_date
)

stage_users_to_redshift = StageToRedshiftOperator(
    task_id='Stage_users',
    dag=dag,
    provide_context=True,
    table="staging_users",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="yelp-data_small",
    s3_key="users_df",
    region="ap-southeast-1",
    data_format="JSON",
    execution_date=start_date
)

stage_checkin_to_redshift = StageToRedshiftOperator(
    task_id='Stage_checkin',
    dag=dag,
    provide_context=True,
    table="staging_checkin",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="yelp-data_small",
    s3_key="checkin_df",
    region="ap-southeast-1",
    data_format="CSV",
    execution_date=start_date
)

load_reviews_table = LoadFactOperator(
    task_id='Load_reviews_fact_table',
    dag=dag,
    provide_context=True,
    table="reviews",
    aws_credentials_id="aws_credentials",
    redshift_conn_id='redshift',
    sql_stmt=SqlQueries.reviews_table_insert
)

load_restaurants_dimension_table = LoadDimensionOperator(
    task_id='Load_restaurants_dim_table',
    dag=dag,
    table="restaurants",
    redshift_conn_id="redshift",
    append_data=False,
    sql_stmt=SqlQueries.restaurants_table_insert
)

load_users_dimension_table = LoadDimensionOperator(
    task_id='Load_users_dim_table',
    dag=dag,
    table="users",
    redshift_conn_id="redshift",
    append_data=False,
    sql_stmt=SqlQueries.users_table_insert
)

load_date_dimension_table = LoadDimensionOperator(
    task_id='Load_date_dim_table',
    dag=dag,
    table='date_table',
    redshift_conn_id="redshift",
    append_data=False,
    sql_stmt=SqlQueries.date_table_insert
)


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    provide_context=True,
    tables=["reviews", "restaurants", "users", "date_table"],    
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    test_stmt= "SELECT COUNT(*) FROM users WHERE user_id IS NULL",
    result=(0,)
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)


# Task Dependencies

start_operator >> create_tables_task
create_tables_task >> [stage_reviews_to_redshift, stage_restaurants_to_redshift,
                   stage_users_to_redshift, stage_checkin_to_redshift]

[stage_reviews_to_redshift, stage_restaurants_to_redshift,
 stage_users_to_redshift, stage_checkin_to_redshift] >> load_reviews_table

load_reviews_table >> [load_restaurants_dimension_table, load_users_dimension_table, load_date_dimension_table]

[load_restaurants_dimension_table, load_users_dimension_table, load_date_dimension_table] >> run_quality_checks

run_quality_checks >> end_operator