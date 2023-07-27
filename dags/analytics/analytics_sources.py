from datetime import datetime

from airflow.models import DAG
from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator

from common.connections import (
    CLICKHOUSE_CONNECTION_ID,
    GOOGLE_CLOUD_CONNECTION_ID,
    POSTGRES_SCORING_API_CONNECTION_ID,
)
from common.gcs import GCS_TEMP_BUCKET, generate_gcs_object_name
from common.operators import ClickhouseToGCSOperator, GCSToPostgresChunkedOperator
from common.slack_notification import slack_fail_alert

args = {
    'start_date': datetime(2023, 3, 29),
    'on_failure_callback': slack_fail_alert,
    'tags': ['analytics', 'sources'],
    'catchup': False,
}

SQL = '''
INSERT INTO analytics.analytics_sources
SELECT
  tracker_id, 
  type as event_type,
  if(page_utm_source is null, 'empty', page_utm_source) as source,
  toInt32(count()) as count
FROM analytics.tracker_events_prod
WHERE NOT isNull(tracker_id) AND NOT empty(tracker_id)
GROUP BY tracker_id, type, page_utm_source;
'''

with DAG(
    dag_id='analytics_sources_aggregation',
    default_args=args,
    schedule='@hourly',
    max_active_runs=1,
) as dag:
    truncate = ClickHouseOperator(
        task_id='truncate_analytics_sources',
        clickhouse_conn_id=CLICKHOUSE_CONNECTION_ID,
        sql='TRUNCATE TABLE analytics.analytics_sources',
    )

    aggregate = ClickHouseOperator(
        task_id='aggregate_analytics_sources',
        clickhouse_conn_id=CLICKHOUSE_CONNECTION_ID,
        sql=SQL,
    )

    upload_task_id = 'upload_aggregated_data_to_gcs'
    object_name = generate_gcs_object_name(
        dag.dag_id, upload_task_id, 'analytics_sources'
    )

    upload = ClickhouseToGCSOperator(
        task_id=upload_task_id,
        sql='SELECT * FROM analytics.analytics_sources',
        clickhouse_conn_id=CLICKHOUSE_CONNECTION_ID,
        clickhouse_database='analytics',
        gcp_conn_id=GOOGLE_CLOUD_CONNECTION_ID,
        bucket_name=GCS_TEMP_BUCKET,
        object_name=object_name,
    )

    insert = GCSToPostgresChunkedOperator(
        task_id='insert_aggregated_data_to_pg',
        postgres_conn_id=POSTGRES_SCORING_API_CONNECTION_ID,
        postgres_database='scoring_api',
        postgres_table='analytics_events_sources',
        gcp_conn_id=GOOGLE_CLOUD_CONNECTION_ID,
        bucket_name=GCS_TEMP_BUCKET,
        upload_task_id=upload_task_id,
        on_conflict_index_columns=['tracker_id', 'event_type', 'source'],
        chunk_size=1024 * 256,
    )

    truncate >> aggregate >> upload >> insert
