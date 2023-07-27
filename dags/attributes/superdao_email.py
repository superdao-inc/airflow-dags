from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator

from common.connections import (
    CLICKHOUSE_CONNECTION_ID,
    GOOGLE_CLOUD_CONNECTION_ID,
    POSTGRES_ROBOTS_CONNECTION_ID,
    POSTGRES_SUPERDAO_CONNECTION_ID,
)
from common.gcs import GCS_TEMP_BUCKET, generate_gcs_object_name
from common.operators import GCSToClickhouseOperator, PostgresToGCSOperator
from common.slack_notification import slack_fail_alert

default_args = {
    'start_date': datetime(2022, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'on_failure_callback': slack_fail_alert,
    # recommended for DBT
    'max_active_runs': 1,
    'profiles_dir': '/opt/airflow/secrets',
    'dir': '/opt/dbt_clickhouse',
}

dag_id = 'superdao_email_dag'

email_sources = [
    {
        "name": "superdao_app",
        "connection": POSTGRES_SUPERDAO_CONNECTION_ID,
        "db": "superdao",
        "fields": 'id, email, lower("walletAddress") as address',
        "table": "users",
    },
    {
        "name": "robots_app",
        "connection": POSTGRES_ROBOTS_CONNECTION_ID,
        "db": "robot",
        "fields": 'id, email, lower("walletAddress") as address',
        "table": "users",
    },
]

clickhouse_table = {"database": 'off_chain', 'table': 'superdao_users'}


with DAG(
    dag_id=dag_id,
    description='DAG for testing ETH and POLYGON data',
    start_date=days_ago(1),
    default_args=default_args,
    schedule='@daily',
) as dag:

    def upload_task_id(src):
        return f"extract_superdao_emails_{src['name']}"

    extract_emails_operators = [
        PostgresToGCSOperator(
            task_id=upload_task_id(src),
            sql=f"SELECT {src['fields']} FROM {src['table']} WHERE email IS NOT NULL AND email <> ''",
            postgres_conn_id=src['connection'],
            postgres_database=src['db'],
            gcp_conn_id=GOOGLE_CLOUD_CONNECTION_ID,
            bucket_name=GCS_TEMP_BUCKET,
            object_name=generate_gcs_object_name(
                dag_id=dag_id, task_id='extract_superdao_emails', name=src['name']
            ),
        )
        for src in email_sources
    ]

    insert_emails_to_ch_operators = [
        GCSToClickhouseOperator(
            task_id=f"insert_superdao_emails_{src['name']}",
            clickhouse_conn_id=CLICKHOUSE_CONNECTION_ID,
            clickhouse_database=clickhouse_table['database'],
            clickhouse_table=clickhouse_table['table'],
            gcp_conn_id=GOOGLE_CLOUD_CONNECTION_ID,
            bucket_name=GCS_TEMP_BUCKET,
            upload_task_id=upload_task_id(src),
            columns_mapper={},
        )
        for src in email_sources
    ]

    truncate_ch_table = ClickHouseOperator(
        task_id='truncate_off_chain_superdao_users',
        clickhouse_conn_id=CLICKHOUSE_CONNECTION_ID,
        database=clickhouse_table['database'],
        sql=f"TRUNCATE TABLE {clickhouse_table['database']}.{clickhouse_table['table']}",
    )

    deduplicate = ClickHouseOperator(
        task_id="deduplicate_off_chain_superdao_users",
        clickhouse_conn_id=CLICKHOUSE_CONNECTION_ID,
        database=clickhouse_table['database'],
        sql=f"OPTIMIZE TABLE {clickhouse_table['database']}.{clickhouse_table['table']} DEDUPLICATE",
    )

    (
        extract_emails_operators
        >> truncate_ch_table
        >> insert_emails_to_ch_operators
        >> deduplicate
    )
