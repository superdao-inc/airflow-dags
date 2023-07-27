#!/usr/bin/env python3
import os

from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "google.json"

client = bigquery.Client()

bucket_name = 'superdao-etl-data/ethereum/logs'
bucket_object = 'logs-*.csv.gz'

dataset = "bigquery-public-data.crypto_ethereum.logs"

QUERY = f"""
EXPORT DATA
  OPTIONS (
    uri = 'gs://{bucket_name}/{bucket_object}',
    format = 'JSON',
    overwrite = true,
    compression = 'GZIP')
AS (
  SELECT *
  FROM `{dataset}`
);
"""

print('Querying')
query_job = client.query(QUERY)
query_job.result()

print(f"Exported {dataset} to gs://{bucket_name}/{bucket_object}")
