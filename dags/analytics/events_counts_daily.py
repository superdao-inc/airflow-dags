from datetime import datetime

from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow_dbt.operators.dbt_operator import DbtRunOperator, DbtTestOperator

from common.connections import (
    CLICKHOUSE_CONNECTION_ID,
    GOOGLE_CLOUD_CONNECTION_ID,
    POSTGRES_SCORING_API_CONNECTION_ID,
)
from common.environment import CURRENT_SCHEMA_DBT
from common.gcs import GCS_TEMP_BUCKET, generate_gcs_object_name
from common.operators import (
    GCS_TO_PG_METHOD_COPY_FROM_STDIN,
    ClickhouseToGCSOperator,
    GCSToPostgresChunkedOperator,
)
from common.slack_notification import slack_fail_alert

default_args = {
    "start_date": datetime(2023, 5, 18),
    "on_failure_callback": slack_fail_alert,
    "catchup": False,
    "tags": ["analytics", "sources"],
    # recommended for DBT
    'max_active_runs': 1,
    'profiles_dir': '/opt/airflow/secrets',
    'dir': '/opt/dbt_clickhouse',
}

dag_id = "analytics_events_counts_daily"

with DAG(
    dag_id=dag_id, default_args=default_args, schedule='@hourly', max_active_runs=1
) as dag:
    start = EmptyOperator(task_id="start", trigger_rule=TriggerRule.ALL_DONE)
    finish = EmptyOperator(task_id="finish", trigger_rule=TriggerRule.ALL_DONE)

    dbt_select = "daily_events_count"
    dbt_db = CURRENT_SCHEMA_DBT
    test = DbtTestOperator(task_id="dbt_test_daily_events_count", select=dbt_select)
    run = DbtRunOperator(task_id="dbt_compute_daily_events_count", select=dbt_select)

    upload_to_gcs_task_id = "upload_events_counts_daily_to_gcs"
    upload_to_gcs = ClickhouseToGCSOperator(
        task_id=upload_to_gcs_task_id,
        sql=f'SELECT * FROM {CURRENT_SCHEMA_DBT}.daily_events_count',
        clickhouse_conn_id=CLICKHOUSE_CONNECTION_ID,
        clickhouse_database=CURRENT_SCHEMA_DBT,
        gcp_conn_id=GOOGLE_CLOUD_CONNECTION_ID,
        bucket_name=GCS_TEMP_BUCKET,
        object_name=generate_gcs_object_name(
            dag_id, upload_to_gcs_task_id, f"{dbt_db}_{dbt_select}"
        ),
        queue="kubernetes",
    )

    insert_to_pg = GCSToPostgresChunkedOperator(
        task_id="insert_events_counts_daily_from_gcs",
        postgres_conn_id=POSTGRES_SCORING_API_CONNECTION_ID,
        postgres_database="scoring_api",
        postgres_table='analytics_events_counts',
        gcp_conn_id=GOOGLE_CLOUD_CONNECTION_ID,
        bucket_name=GCS_TEMP_BUCKET,
        upload_task_id=upload_to_gcs_task_id,
        chunk_size=1024 * 256,
        method=GCS_TO_PG_METHOD_COPY_FROM_STDIN,
        queue="kubernetes",
    )

    (start >> run >> test >> upload_to_gcs >> insert_to_pg >> finish)
