import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from transformation import *

## Define the etl function
def etl():
    movies_df = extract_movies_to_df()
    users_df = extract_users_to_df()
    transformed_df = transform_avg_ratings(movies_df, users_df)
    load_df_to_db(transformed_df)

## Define the arguments for the DAG
default_args = {
    'owner': 'louise',
    'start_date': airflow.utils.dates.days_ago(1),
    'depends_on_past': True,
    'email': ['louisek@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=30),
}

## Instantiate the DAG
dag = DAG(dag_id='etl_pipeline',
          default_args=default_args,
          schedule_interval="0 0 * * *")

## Define the ETL task
etl_task = PythonOperator(task_id='etl_task',
                          python_callable=etl,
                          dag=dag)

etl()