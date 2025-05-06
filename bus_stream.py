from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email': ['twoj.email@firma.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='bus_stream',
    default_args=default_args,
    description='Uruchamianie notebooka na istniejącym klastrze w Databricks',
    start_date=datetime(2025, 5, 6),
    catchup=False,
    tags=['databricks', 'etl'],
) as dag:

    run_notebook = DatabricksSubmitRunOperator(
        task_id='run_existing_cluster_notebook',
        databricks_conn_id='databricks_conn',
        existing_cluster_id='0506-103902-gl3frsyg',
        notebook_task={
            'notebook_path': '/Repos/bus_data_project/bus_data_project/silver_bus_stream',
            'base_parameters': {}
        }
    )
