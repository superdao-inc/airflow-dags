import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator

from common.connections import CLICKHOUSE_CONNECTION_ID, GOOGLE_CLOUD_CONNECTION_ID
from common.gcs import (
    GCS_ETL_BUCKET,
    GCS_S3_COMPATIBLE_ENDPOINT,
    GCS_S3_COMPATIBLE_TEMP_BUCKET_ACCESS_KEY_ID,
    GCS_S3_COMPATIBLE_TEMP_BUCKET_SECRET,
)
from common.operators import S3ToClickhouseOperator

FROM_GCS_TO_CLICKHOUSE_DAG_ID = 'from_gcs_to_clickhouse'
args = {
    'start_date': datetime(2022, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'tags': ['utility', 'gcs', 'clickhouse'],
}


with DAG(dag_id=FROM_GCS_TO_CLICKHOUSE_DAG_ID, default_args=args, schedule=None) as dag:
    """
    Example input configurtaion: {
        "bucket": "superdao-etl-data",
        "prefix": "polygon/token_transfers/token_transfers-"
        "delimiter": ".csv.gzip",
        "database": "polygon_data",
        "table": "polygon_data.token_transfers_by_token",
        "format": "CSVWithNames",
    }
    """

    gcs_bucket_objects_list = GCSListObjectsOperator(
        task_id='gcs_bucket_objects_list',
        bucket='{{ params.bucket }}',
        prefix='{{ params.prefix }}',
        delimiter='{{ params.delimiter }}',
        gcp_conn_id=GOOGLE_CLOUD_CONNECTION_ID,
    )

    @task(task_id='insert_objects')
    def insert_objects(**kwargs):
        ti = kwargs['ti']
        objects = ti.xcom_pull(task_ids='gcs_bucket_objects_list', key='return_value')

        for i, obj in enumerate(objects, start=1):
            insert_gcs_objects = S3ToClickhouseOperator(
                task_id=f'insert_{i}',
                clickhouse_conn_id=CLICKHOUSE_CONNECTION_ID,
                clickhouse_database=kwargs['params']['database'],
                clickhouse_table=kwargs['params']['table'],
                gcp_conn_id=GOOGLE_CLOUD_CONNECTION_ID,
                s3_access_key_id=GCS_S3_COMPATIBLE_TEMP_BUCKET_ACCESS_KEY_ID,
                s3_secret_key=GCS_S3_COMPATIBLE_TEMP_BUCKET_SECRET,
                s3_endpoint=GCS_S3_COMPATIBLE_ENDPOINT,
                s3_bucket_name=kwargs['params']['bucket'],
                s3_object_name=obj,
                format=kwargs['params']['format'],
            )
            insert_gcs_objects.execute(context=kwargs)

    gcs_bucket_objects_list >> insert_objects()
