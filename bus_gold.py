from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['twoj.email@firma.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30) 
}

with DAG(
    dag_id='bus_stream_gold_hourly',
    default_args=default_args,
    description='Hourly processing of bus data on existing Databricks cluster',
    schedule_interval='0 * * * *',  # Co godzinę o minucie 0
    start_date=datetime(2025, 5, 6, 0, 0),
    catchup=False,
    max_active_runs=1,
    tags=['databricks', 'bus', 'hourly'],
) as dag:

    process_hourly_data = DatabricksSubmitRunOperator(
        task_id='process_bus_data_hourly',
        databricks_conn_id='databricks_conn',
        existing_cluster_id='0506-103902-gl3frsyg',
        notebook_task={
            'notebook_path': '/Repos/bus_data_project/bus_data_project/gold_bus_data'
        },
        do_xcom_push=True
    )