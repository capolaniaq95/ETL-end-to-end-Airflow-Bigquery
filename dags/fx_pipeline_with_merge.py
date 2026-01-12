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
    logging.info("📊 Cargando datos a BigQuery con MERGE...")

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

    fx_date = pd.to_datetime(date_str).normalize()
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

    # Initialize BigQuery client
    client = bigquery.Client(project=GCP_PROJECT_ID)
    table_id = f"{GCP_PROJECT_ID}.{GCP_DATASET_ID}.{GCP_TABLE_ID}"
    
    # Create temporary table for MERGE operation
    temp_table_id = f"{GCP_PROJECT_ID}.{GCP_DATASET_ID}.temp_fx_rates_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # Load data to temporary table
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
        schema=[
            bigquery.SchemaField("date", "TIMESTAMP"),
            bigquery.SchemaField("from_cur", "STRING"),
            bigquery.SchemaField("to_cur", "STRING"),
            bigquery.SchemaField("rate", "FLOAT"),
            bigquery.SchemaField("timestamp", "TIMESTAMP")
        ]
    )
    
    job = client.load_table_from_dataframe(df, temp_table_id, job_config=job_config)
    job.result()
    logging.info(f"Loaded {len(df)} records to temporary table: {temp_table_id}")
    
    # Execute MERGE statement for idempotency
    merge_query = f"""
    MERGE `{table_id}` T
    USING `{temp_table_id}` S
    ON T.date = S.date 
       AND T.from_cur = S.from_cur 
       AND T.to_cur = S.to_cur
    WHEN MATCHED THEN
        UPDATE SET 
            T.rate = S.rate,
            T.timestamp = S.timestamp
    WHEN NOT MATCHED THEN
        INSERT (date, from_cur, to_cur, rate, timestamp)
        VALUES (S.date, S.from_cur, S.to_cur, S.rate, S.timestamp)
    """
    
    query_job = client.query(merge_query)
    query_job.result()
    logging.info("MERGE operation completed successfully")
    
    # Clean up temporary table
    client.delete_table(temp_table_id)
    logging.info(f"Cleaned up temporary table: {temp_table_id}")
    
    return True


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

    # Task 2: Insert data into BigQuery using MERGE
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

    # Use MERGE for average rate table as well
    avg_table_id = f"{GCP_PROJECT_ID}.{GCP_DATASET_ID}.{GCP_AVERAGE_TABLE_ID}"
    
    merge_avg_query = f"""
    MERGE `{avg_table_id}` T
    USING (SELECT 
           TIMESTAMP('{date}') as date,
           '{cur_from}' as from_cur,
           '{cur_to}' as to_cur,
           {average} as avg_rate) S
    ON T.date = S.date 
       AND T.from_cur = S.from_cur 
       AND T.to_cur = S.to_cur
    WHEN MATCHED THEN
        UPDATE SET 
            T.avg_rate = S.avg_rate
    WHEN NOT MATCHED THEN
        INSERT (date, from_cur, to_cur, avg_rate)
        VALUES (S.date, S.from_cur, S.to_cur, S.avg_rate)
    """
    
    query_job = client.query(merge_avg_query)
    query_job.result()
    logging.info("MERGE operation for average rates completed successfully")

    return True


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