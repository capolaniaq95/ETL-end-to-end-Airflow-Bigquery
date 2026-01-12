# ETL-end-to-end-Airflow-Bigquery

## Overview

An end-to-end ETL pipeline implemented with Apache Airflow that fetches EUR→USD exchange rates from the Frankfurter API and persists raw and aggregated results into BigQuery.

- Raw rates are stored in: `airflow_challenge.exchange_rate`
- Daily aggregated averages (latest analysis of the day overwrites previous) are stored in: `airflow_challenge.exchange_rate_report`

This repository demonstrates scheduling, idempotent ingestion, aggregation/upsert patterns and Google Cloud integration.

## Architecture

- Airflow DAG 1 (every minute, Mon–Fri): calls Frankfurter API to fetch the current EUR→USD rate and inserts a row into `exchange_rate`.
- Airflow DAG 2 (every 5 minutes, Mon–Fri): computes the average rate for the current day so far and upserts the result into `exchange_rate_report` (overwriting any previous analysis for the same date).

For local testing, frequency is reduced (every minute / every five minutes) to allow quick verification.

## BigQuery setup

Create dataset (replace PROJECT_ID and LOCATION as needed):

bq --location=US mk --dataset PROJECT_ID:airflow_challenge

Create tables (SQL):

CREATE TABLE IF NOT EXISTS `PROJECT_ID.airflow_challenge.exchange_rate` (
    timestamp TIMESTAMP,
    date TIMESTAMP,
    from_cur STRING,
    to_cur STRING,
    rate FLOAT64
);

CREATE TABLE IF NOT EXISTS `PROJECT_ID.airflow_challenge.exchange_rate_report` (
    date TIMESTAMP,
    from_cur STRING,
    to_cur STRING,
    avg_rate FLOAT64
);

Notes:
- Consider partitioning by DATE(date) and clustering by from_cur,to_cur for better performance on large datasets.
- Ensure appropriate IAM roles for the service account that Airflow uses (BigQuery Data Editor / JobUser).

## Upsert (aggregation) pattern

Example MERGE to compute and save the current-day average into the report table (run from Airflow BigQueryOperator or client):

MERGE `PROJECT_ID.airflow_challenge.exchange_rate_report` T
USING (
    SELECT
        TIMESTAMP_TRUNC(date, DAY) AS date,
        from_cur, to_cur,
        AVG(rate) AS avg_rate
    FROM `PROJECT_ID.airflow_challenge.exchange_rate`
    WHERE DATE(date) = CURRENT_DATE()
    GROUP BY TIMESTAMP_TRUNC(date, DAY), from_cur, to_cur
) S
ON T.date = S.date AND T.from_cur = S.from_cur AND T.to_cur = S.to_cur
WHEN MATCHED THEN
    UPDATE SET avg_rate = S.avg_rate
WHEN NOT MATCHED THEN
    INSERT (date, from_cur, to_cur, avg_rate) VALUES (S.date, S.from_cur, S.to_cur, S.avg_rate);

This ensures the latest analysis for the day overwrites previous ones.

## Frankfurter API (sample)

Endpoint to fetch latest EUR->USD rate:

https://api.frankfurter.app/latest?from=EUR&to=USD

Sample response:

{
    "amount": 1.0,
    "base": "EUR",
    "date": "2023-11-10",
    "rates": { "USD": 1.0647 }
}

Map response to table columns:
- timestamp : ingestion time (now)
- date : API date (cast to TIMESTAMP)
- from_cur : "EUR"
- to_cur : "USD"
- rate : 1.0647

## Airflow integration

Recommended components:
- Use GoogleCloud BigQueryHook / BigQueryInsertJobOperator or BigQueryInsertJobOperator with service account credentials.
- Create an Airflow connection (e.g., `google_cloud_default`) with a service account JSON or use Workload Identity / GKE metadata when running on GCP.
- Use Variables or DAG params for PROJECT_ID, DATASET, and table names.
- DAG schedules:
    - fetch_dag: schedule_interval='*/1 * * * 1-5'
    - aggregate_dag: schedule_interval='*/5 * * * 1-5'

Testing:
- Local: set GOOGLE_APPLICATION_CREDENTIALS and run `airflow dags test <dag_id> <execution_date>`
- CI: mock BigQuery with integration tests or use a test project.

## Design notes & trade-offs

- Raw table + periodic aggregation:
    - Pros: full historical data, flexible re-aggregation/backfills.
    - Cons: storage and compute overhead.
- Alternative: stream and compute aggregate on read (realtime query) or use Dataflow/Beam for windowed aggregates.
- Idempotency: avoid duplicate rows by ensuring each ingestion has a reliable timestamp and dedupe logic (or use unique keys + dedupe query).
- Overwrite policy: MERGE keeps the report table small and always reflects the latest intra-day average.

## Running & troubleshooting

1. Ensure Airflow can authenticate to GCP (service account with BigQuery permissions).
2. Create dataset and tables as above.
3. Configure Airflow connection and variables (PROJECT_ID, DATASET).
4. Deploy DAGs to your Airflow environment and enable them.
5. Monitor DAG runs and BigQuery jobs; inspect logs if failures occur.

## Encouragement

This is an open-ended challenge: try different ingestion frequencies, aggregation windows, storage/partition schemes, and failure recovery strategies. Compare costs, latency, and complexity for each design.

License / Contributing / Contacts
- Add your license and contribution instructions as required.
- Submit PRs for improvements or new approaches.