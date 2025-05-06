from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeStorageV2Hook
import requests
import json
import os
import tempfile

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

def fetch_bus_data(line=None, **context):

    connection = BaseHook.get_connection('um_warszawa_api')
    api_key = connection.password
    
    url = "https://api.um.warszawa.pl/api/action/busestrams_get/"
    params = {
        "resource_id": "f2e5503e-927d-4ad3-9500-4ab9e55deb59",
        "apikey": api_key,
        "type": 1
    }
    
    response = requests.get(url, params=params)
    response.raise_for_status()
    
    data = response.json()
    
    if data.get("result") == "Błędna metoda lub parametry wywołania":
        raise ValueError("Nieprawidłowe parametry zapytania")
    
    buses = data.get("result", [])
    
    if line:
        buses = [b for b in buses if b.get("Lines") == line]
    
    context['ti'].log.info(f"Pobrano dane o {len(buses)} autobusach")
    return buses

def save_to_adls(**context):
    ti = context['ti']
    buses = ti.xcom_pull(task_ids='fetch_bus_data')
    
    if not buses:
        raise ValueError("Brak danych do zapisania")
    
    # Pobierz połączenie do ADLS Gen2
    adls_hook = AzureDataLakeStorageV2Hook(adls_conn_id='adls_connection_string')
    
    # Generuj nazwę pliku z timestampem
    now = datetime.now().strftime("%Y%m%d_%H%M%S")
    line = context['params'].get('line', 'all')
    file_path = f"warsaw_buses/{line}/bus_data_{now}.json"
    
    # Convert data to JSON string
    data = json.dumps(buses, indent=2)
    
    # Create file client and upload data
    file_system_client = adls_hook.get_file_system('apidata')
    file_client = file_system_client.get_file_client(file_path)
    
    try:
        file_client.create_file()
        file_client.append_data(data, 0, len(data))
        file_client.flush_data(len(data))
        ti.log.info(f"Pomyślnie zapisano dane do: {file_path} w kontenerze apidata")
    except Exception as e:
        ti.log.error(f"Błąd podczas zapisywania danych: {str(e)}")
        raise

with DAG(
    'warsaw_buses_to_adls_fixed2',
    default_args=default_args,
    description='Pobiera dane o autobusach z UM Warszawa i zapisuje do Azure Data Lake Gen2 (poprawiona wersja)',
    schedule_interval=timedelta(minutes=15),
    start_date=datetime(2025, 5, 5),
    catchup=False,
    tags=['warsaw', 'transport', 'azure', 'fixed'],
) as dag:
    
    fetch_data = PythonOperator(
        task_id='fetch_bus_data',
        python_callable=fetch_bus_data,
        op_kwargs={'line': '194'},
        provide_context=True,
    )
    
    save_data = PythonOperator(
        task_id='save_to_adls',
        python_callable=save_to_adls,
        provide_context=True,
        params={'line': '194'},
    )
    
    fetch_data >> save_data