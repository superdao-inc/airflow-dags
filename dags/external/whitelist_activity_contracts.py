from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

from common.connections import (
    CLICKHOUSE_CONNECTION_ID,
    GOOGLE_CLOUD_CONNECTION_ID,
    POSTGRES_SCORING_API_CONNECTION_ID,
)
from common.gcs import GCS_TEMP_BUCKET, generate_gcs_object_name
from common.operators import GCSToClickhouseOperator, PostgresToGCSOperator
from common.slack_notification import slack_fail_alert

args = {
    'start_date': datetime(2023, 4, 26),
    'on_failure_callback': slack_fail_alert,
    'tags': ['external', 'whitelist_activity_contracts'],
    'catchup': False,
}

dag_id = "external_whitelist_activity_contracts"

with DAG(dag_id=dag_id, default_args=args, max_active_runs=1, schedule='0 * * * *'):
    upload_task_id = 'upload_whitelist_activity_contracts_to_gcs'

    start = EmptyOperator(task_id="start", trigger_rule=TriggerRule.ALL_DONE)
    finish = EmptyOperator(task_id="finish", trigger_rule=TriggerRule.ALL_DONE)

    pg_to_gcs = PostgresToGCSOperator(
        task_id=upload_task_id,
        sql="SELECT lower(address) as address FROM input_activity_metadata",
        postgres_conn_id=POSTGRES_SCORING_API_CONNECTION_ID,
        postgres_database='scoring_api',
        gcp_conn_id=GOOGLE_CLOUD_CONNECTION_ID,
        bucket_name=GCS_TEMP_BUCKET,
        object_name=generate_gcs_object_name(
            dag_id,
            upload_task_id,
            "whitelist_activity_contracts",
        ),
    )

    insert_tasks = []
    for ch_db in ["eth_data", "polygon_data"]:
        insert_tasks.append(
            GCSToClickhouseOperator(
                task_id=f"insert_whitelist_activity_contracts_{ch_db}",
                clickhouse_conn_id=CLICKHOUSE_CONNECTION_ID,
                clickhouse_database=ch_db,
                clickhouse_table='whitelist_contracts',
                gcp_conn_id=GOOGLE_CLOUD_CONNECTION_ID,
                bucket_name=GCS_TEMP_BUCKET,
                upload_task_id=upload_task_id,
                columns_mapper={},
                truncate_table=True,
            )
        )

    start >> pg_to_gcs >> insert_tasks >> finish
