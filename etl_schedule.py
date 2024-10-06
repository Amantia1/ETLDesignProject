from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from etl import etl_process  # Import the ETL function

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 4),
    'retries': 1,
}

with DAG('etl_csvpostgress_pipeline', default_args=default_args, schedule_interval='@hourly', catchup=False) as dag:

    etl_task = PythonOperator(
        task_id='etl_process',
        python_callable=etl_process,
    )