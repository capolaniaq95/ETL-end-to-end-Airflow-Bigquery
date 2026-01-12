from airflow import DAG  # type: ignore
from airflow.providers.standard.operators.python import PythonOperator  # type: ignore
from datetime import datetime
import requests
import logging
import os
import pandas as pd
from google.cloud import bigquery
import numpy as np
import ast

# Variables de entorno
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID', 'tu-proyecto-gcp')
GCP_DATASET_ID = os.getenv('GCP_DATASET_ID', 'datos_ventas')
GCP_TABLE_ID = os.getenv('GCP_TABLE_ID', 'ventas_procesadas')
GCP_AVERAGE_TABLE_ID = os.getenv('GCP_AVERAGE_TABLE_ID', 'exchange_rate_report')

FX_DEFAULTS = {
    'to': 'USD',
    'url': "https://api.frankfurter.dev/v1/latest?symbols={to}"
}

def fetch_fx_rate(**kwargs):
    url = kwargs['url'].format(to=kwargs['to'])
    logging.info(f"Fetching FX rate from URL: {url}")
    response = requests.get(url)
    data = response.json()
    rate = data['rates'][kwargs['to']]
    logging.info(f"Fetched FX rate: 1 EUR = {rate} {kwargs['to']}")
    return data


def insert_to_bigquery(**context):

    logging.info("📊 Cargando datos a BigQuery...")

    ti = context['task_instance']
    data = ti.xcom_pull(task_ids='fetch_fx_rate')
    logging.info(f"Data pulled from XCom: {data}")
    
    if not data:
        logging.warning("No data pulled from fetch_fx_rate; nothing to insert.")
        return 0    

    # Construir DataFrame a partir del JSON devuelto por la API
    date_str = data.get("date")
    base = data.get("base")
    rates = data.get("rates", {})
    rows = []

    fx_date = pd.to_datetime(date_str).normalize()  # Converts to datetime, sets time to 00:00:00
    current_timestamp = pd.Timestamp.now(tz='UTC')

    for target_currency, rate in rates.items():
        rows.append({
            "date": fx_date,
            "from_cur": base,
            "to_cur": target_currency,
            "rate": float(rate),
            "timestamp": current_timestamp
        })
    df = pd.DataFrame(rows)

    logging.info(f"DataFrame constructed with {len(df)} rows")
    logging.info(df.head())

    # Initialize BigQuery client
    client = bigquery.Client(project=GCP_PROJECT_ID)
    
    # Configurar tabla destino
    table_id = f"{GCP_PROJECT_ID}.{GCP_DATASET_ID}.{GCP_TABLE_ID}"
    
    # Check for existing records to ensure idempotency
    existing_records_query = f"""
    SELECT COUNT(*) as count 
    FROM `{table_id}` 
    WHERE DATE(date) = DATE('{fx_date}') 
    AND from_cur = '{base}' 
    AND to_cur IN ({','.join([f"'{currency}'" for currency in rates.keys()])})
    """
    
    query_job = client.query(existing_records_query)
    existing_count = list(query_job)[0].count
    
    if existing_count > 0:
        logging.info(f"Found {existing_count} existing records for date {fx_date}. Skipping insertion to maintain idempotency.")
        return True
    
    # Configurar job de carga con WRITE_TRUNCATE para reemplazar datos del mismo día
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=True,
        schema=[
            bigquery.SchemaField("date", "TIMESTAMP"),
            bigquery.SchemaField("from_cur", "STRING"),
            bigquery.SchemaField("to_cur", "STRING"),
            bigquery.SchemaField("rate", "FLOAT"),
            bigquery.SchemaField("timestamp", "TIMESTAMP")
        ]
    )
    
    logging.info(f"Inserting {len(df)} records to BigQuery table: {table_id}")
    
    job = client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )
    
    # Wait for the job to complete
    job.result()
    
    logging.info(f"Successfully inserted {len(df)} records to BigQuery")
    if len(df) > 0:
        logging.info("Data insertion completed successfully.")
        return True
    return False


with DAG(
    dag_id='FXRate_Reporting_Pipeline',
    default_args={},
    schedule="0 * * * 1-5",
    start_date=datetime(2024, 10, 27),
    catchup=False
) as dag:

    # Task 1: Extract data from API (pass params via op_kwargs)
    fetch_fx_rate_task = PythonOperator(
        task_id='fetch_fx_rate',
        python_callable=fetch_fx_rate,
        op_kwargs=FX_DEFAULTS
    )

    # Task 2: Insert data into BigQuery (reads XCom from fetch task)
    insert_to_bigquery_task = PythonOperator(
        task_id='insert_to_bigquery',
        python_callable=insert_to_bigquery
    )

    # Define task dependencies
    fetch_fx_rate_task >> insert_to_bigquery_task
    

def query_exchange_rate(**context):
    logging.info("Querying exchange rates from BigQuery...")

    ti = context['task_instance']
    data = ti.xcom_pull(task_ids='insert_to_bigquery')

    if not data:
        logging.warning("error in the previous task, not information added")
    
    client = bigquery.Client(project=GCP_PROJECT_ID)
    table_id = f"{GCP_PROJECT_ID}.{GCP_DATASET_ID}.{GCP_TABLE_ID}"

    date = datetime.now().date()
    query = f"SELECT rate FROM `{table_id}` ORDER BY timestamp DESC LIMIT 10"

    query_job = client.query(query)
    rates_list = [row.rate for row in query_job]

    rates_list = np.array(rates_list, dtype=float)

    logging.info("Latest exchange rates from BigQuery:")
    logging.info(rates_list)

    average = np.mean(rates_list)
    average = round(average, 4)
    date = pd.Timestamp.now(tz='UTC')
    cur_from = 'EUR'
    cur_to = 'USD'

    row = {
        "date": date,
        "from_cur": cur_from,
        "to_cur": cur_to,
        "avg_rate": float(average),
    }
    df = pd.DataFrame([row])

    logging.info(f"Average exchange rate DataFrame:")
    
    avg_table_id = f"{GCP_PROJECT_ID}.{GCP_DATASET_ID}.{GCP_AVERAGE_TABLE_ID}"
    
    # Check for existing record for current date to ensure idempotency
    existing_avg_query = f"""
    SELECT COUNT(*) as count 
    FROM `{avg_table_id}` 
    WHERE DATE(date) = DATE('{date}') 
    AND from_cur = '{cur_from}' 
    AND to_cur = '{cur_to}'
    """
    
    query_job = client.query(existing_avg_query)
    existing_avg_count = list(query_job)[0].count
    
    if existing_avg_count > 0:
        logging.info(f"Found existing average rate record for date {date}. Skipping insertion to maintain idempotency.")
        return True
    
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=True,
        schema=[
            bigquery.SchemaField("date", "TIMESTAMP"),
            bigquery.SchemaField("from_cur", "STRING"),
            bigquery.SchemaField("to_cur", "STRING"),
            bigquery.SchemaField("avg_rate", "FLOAT"),
        ]
    )
    
    logging.info(f"Inserting {len(df)} records to BigQuery table: {avg_table_id}")
    
    job = client.load_table_from_dataframe(
        df, avg_table_id, job_config=job_config
    )
    
    # Wait for the job to complete
    job.result()

    if len(df) > 0:
        logging.info("Average rate insertion completed successfully.")
        return True
    return False


with DAG (
    dag_id='FXRate_Reporting_Pipeline_v2',
    default_args={},
    schedule = "0 6 * * 1-5",
    start_date = datetime (2024, 10, 27),
    catchup = False
) as dag:
    
    #task 1: query data from BigQuery
    load_to_exchange_rate_table = PythonOperator(
        task_id='query_exchange_rate',
        python_callable=query_exchange_rate
    )

    load_to_exchange_rate_table