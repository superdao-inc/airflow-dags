from datetime import datetime

from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator
from airflow_dbt.operators.dbt_operator import DbtTestOperator

from common.connections import (
    CLICKHOUSE_CONNECTION_ID,
    GOOGLE_CLOUD_CONNECTION_ID,
    POSTGRES_SCORING_API_CONNECTION_ID,
    SUPERDAO_APP_API_CONN_ID,
    SUPERDAO_APP_HTTP_SECRET_ID,
)
from common.environment import CURRENT_SCHEMA_DBT
from common.gcs import GCS_TEMP_BUCKET, generate_gcs_object_name
from common.operators import (
    GCS_TO_PG_METHOD_COPY_FROM_STDIN,
    ClickhouseToGCSOperator,
    GCSToPostgresChunkedOperator,
)
from common.queries.refresh_matview import test_all_attr
from common.slack_notification import slack_fail_alert

args = {
    "start_date": datetime(2023, 3, 30),
    "max_active_runs": 3,
    "on_failure_callback": slack_fail_alert,
    "tags": ["attribute", "pg_replication"],
    "catchup": False,
    "retries": 2,
    "retry_delay": 60,
    "profiles_dir": "/opt/airflow/secrets",
    "dir": "/opt/dbt_clickhouse",
}

CREATE_ADDRESS_SET_SQL = """
    CREATE TABLE IF NOT EXISTS {ch_db}.replicated_wallets_set (address String)
    ENGINE = Set();
"""

COLLECT_REPLICATED_ADDRESSES_SQL = """
        INSERT INTO {ch_db}.replicated_wallets_set (address)
        SELECT DISTINCT address FROM (
            SELECT wallet AS address FROM {dbt_schema}.eth_wallet_tokens
            UNION ALL
            SELECT wallet AS address FROM {dbt_schema}.polygon_wallet_tokens
            UNION ALL
            SELECT address FROM analytics.wallet_last_events
            UNION ALL
            SELECT address FROM {dbt_schema}.all_audiences
        )
"""

SELECT_RECORDS_SQL = """
    SELECT * FROM {ch_db}.{table}
    WHERE {wallet_column} IN {ch_db}.replicated_wallets_set
"""

SUPERDAO_APP_CACHE_INVALIDATION_ENDPOINT = '/refresh_audiencies_insights_cache'
superdao_app_api_key = Variable.get(SUPERDAO_APP_HTTP_SECRET_ID, '')

tables_to_replicate = [
    # dbt
    {
        "ch_db": "dbt",
        "pg_table": "dictionary",
        "wallet_column": None,
        "index_columns": ["key"],
        "ch_table": "eth_usdt_price_dict",
    },
    {
        "ch_db": "dbt",
        "pg_table": "off_chain_attr_email",
        "wallet_column": "address",
        "index_columns": ["address"],
        "ch_table": "attr_superdao_emails",
    },
    # dbt
    {
        "ch_db": "dbt",
        "pg_table": "analytics_wallet_last_events",
        "wallet_column": "address",
        "index_columns": ["tracker_id", "address"],
        "ch_table": "wallet_last_events",
    },
    # dbt
    {
        "ch_db": "dbt",
        "pg_table": "off_chain_attr_ens_name",
        "wallet_column": "address",
        "index_columns": ["address"],
        "ch_table": "attr_ens_name",
    },
    {
        "ch_db": "dbt",
        "pg_table": "eth_attr_wallet_usd_cap",
        "wallet_column": "address",
        "index_columns": ["address"],
        "ch_table": "eth_attr_wallet_usd_cap",
    },
    {
        "ch_db": "dbt",
        "pg_table": "polygon_attr_wallet_usd_cap",
        "wallet_column": "address",
        "index_columns": ["address"],
        "ch_table": "polygon_attr_wallet_usd_cap",
    },
    {
        "ch_db": "dbt",
        "pg_table": "polygon_attr_labels",
        "wallet_column": "address",
        "index_columns": ["address"],
        "ch_table": "polygon_attr_labels",
    },
    {
        "ch_db": "dbt",
        "pg_table": "polygon_attr_nfts_count",
        "wallet_column": "address",
        "index_columns": ["address"],
        "ch_table": "polygon_attr_nfts_count",
    },
    {
        "ch_db": "dbt",
        "pg_table": "eth_attr_labels",
        "wallet_column": "address",
        "index_columns": ["address"],
        "ch_table": "eth_attr_labels",
    },
    {
        "ch_db": "dbt",
        "pg_table": "eth_attr_whitelist_activity",
        "wallet_column": "address",
        "index_columns": ["address"],
        "ch_table": "eth_attr_whitelist_activity",
    },
    {
        "ch_db": "dbt",
        "pg_table": "polygon_attr_whitelist_activity",
        "wallet_column": "address",
        "index_columns": ["address"],
        "ch_table": "polygon_attr_whitelist_activity",
    },
    {
        "ch_db": "dbt",
        "pg_table": "off_chain_attr_twitter_avatar_url",
        "wallet_column": "address",
        "index_columns": ["address"],
        "ch_table": "off_chain_attr_twitter_avatar_url",
    },
    {
        "ch_db": "dbt",
        "pg_table": "off_chain_attr_twitter_followers_count",
        "wallet_column": "address",
        "index_columns": ["address"],
        "ch_table": "off_chain_attr_twitter_followers_count",
    },
    {
        "ch_db": "dbt",
        "pg_table": "off_chain_attr_twitter_url",
        "wallet_column": "address",
        "index_columns": ["address"],
        "ch_table": "off_chain_attr_twitter_url",
    },
    {
        "ch_db": "dbt",
        "pg_table": "off_chain_attr_twitter_username",
        "wallet_column": "address",
        "index_columns": ["address"],
        "ch_table": "off_chain_attr_twitter_username",
    },
    {
        "ch_db": "dbt",
        "pg_table": "off_chain_attr_twitter_location",
        "wallet_column": "address",
        "index_columns": ["address"],
        "ch_table": "off_chain_attr_twitter_location",
    },
    {
        "ch_db": "dbt",
        "pg_table": "off_chain_attr_twitter_bio",
        "wallet_column": "address",
        "index_columns": ["address"],
        "ch_table": "off_chain_attr_twitter_bio",
    },
    {
        "ch_db": "dbt",
        "pg_table": "eth_attr_last_month_in_volume",
        "wallet_column": "address",
        "index_columns": ["address"],
        "ch_table": "eth_attr_last_month_in_volume",
    },
    {
        "ch_db": "dbt",
        "pg_table": "eth_attr_last_month_out_volume",
        "wallet_column": "address",
        "index_columns": ["address"],
        "ch_table": "eth_attr_last_month_out_volume",
    },
    {
        "ch_db": "dbt",
        "pg_table": "eth_attr_created_at",
        "wallet_column": "address",
        "index_columns": ["address"],
        "ch_table": "eth_attr_created_at",
    },
    {
        "ch_db": "dbt",
        "pg_table": "eth_attr_last_month_tx_count",
        "wallet_column": "address",
        "index_columns": ["address"],
        "ch_table": "eth_attr_last_month_tx_count",
    },
    {
        "ch_db": "dbt",
        "pg_table": "eth_attr_nfts_count",
        "wallet_column": "address",
        "index_columns": ["address"],
        "ch_table": "eth_attr_nfts_count",
    },
    {
        "ch_db": "dbt",
        "pg_table": "polygon_attr_created_at",
        "wallet_column": "address",
        "index_columns": ["address"],
        "ch_table": "polygon_attr_created_at",
    },
    {
        "ch_db": "dbt",
        "pg_table": "polygon_attr_last_month_in_volume",
        "wallet_column": "address",
        "index_columns": ["address"],
        "ch_table": "polygon_attr_last_month_in_volume",
    },
    {
        "ch_db": "dbt",
        "pg_table": "polygon_attr_last_month_out_volume",
        "wallet_column": "address",
        "index_columns": ["address"],
        "ch_table": "polygon_attr_last_month_out_volume",
    },
    {
        "ch_db": "dbt",
        "pg_table": "polygon_attr_last_month_tx_count",
        "wallet_column": "address",
        "index_columns": ["address"],
        "ch_table": "polygon_attr_last_month_tx_count",
    },
    {
        "ch_db": "dbt",
        "pg_table": "eth_attr_tx_count",
        "wallet_column": "address",
        "index_columns": ["address"],
        "ch_table": "eth_attr_tx_count",
    },
    {
        "ch_db": "dbt",
        "pg_table": "polygon_attr_tx_count",
        "wallet_column": "address",
        "index_columns": ["address"],
        "ch_table": "polygon_attr_tx_count",
    },
]


def build_replication_dag(ch_db, schedule_interval):
    dag_id = f"attributes_replication_{ch_db}"

    @dag(
        dag_id=dag_id,
        default_args=args,
        schedule_interval=schedule_interval,
        template_searchpath=[
            "/opt/airflow/common/queries",
        ],
    )
    def replication_dag():
        start = EmptyOperator(task_id="start")
        finish = EmptyOperator(task_id="finish")

        if ch_db in ["polygon_data", "eth_data"]:
            dbt_operator = DbtTestOperator(
                task_id=f"dbt_test_{ch_db}", select="tag:attr_labels"
            )
        else:
            dbt_operator = EmptyOperator(task_id=f"dbt_test_{ch_db}")

        create_replicated_wallets_set = ClickHouseOperator(
            task_id=f"create_replicated_wallets_set_{ch_db}",
            clickhouse_conn_id=CLICKHOUSE_CONNECTION_ID,
            database=ch_db,
            sql=CREATE_ADDRESS_SET_SQL.format(ch_db=ch_db),
        )

        collect_replicated_addresses = ClickHouseOperator(
            task_id=f"collect_replicated_addresses_{ch_db}",
            clickhouse_conn_id=CLICKHOUSE_CONNECTION_ID,
            database=ch_db,
            sql=COLLECT_REPLICATED_ADDRESSES_SQL.format(
                ch_db=ch_db, dbt_schema=CURRENT_SCHEMA_DBT
            ),
        )

        for table_data in tables_to_replicate:
            if table_data["ch_db"] != ch_db:
                continue
            ch_table = table_data["ch_table"]
            pg_table = table_data["pg_table"]
            wallet_column = table_data["wallet_column"]
            index_columns = table_data["index_columns"]
            limit = table_data.get("limit", None)
            upload_task_id = f"upload_to_gcs_{ch_db}_{ch_table}"

            sql = (
                SELECT_RECORDS_SQL.format(
                    ch_db=ch_db, table=ch_table, wallet_column=wallet_column
                )
                if wallet_column
                else f"SELECT * FROM {ch_db}.{ch_table}"
            )

            if limit:
                sql += f" LIMIT {limit}"

            upload_task = ClickhouseToGCSOperator(
                task_id=upload_task_id,
                sql=sql,
                clickhouse_conn_id=CLICKHOUSE_CONNECTION_ID,
                clickhouse_database=ch_db,
                gcp_conn_id=GOOGLE_CLOUD_CONNECTION_ID,
                bucket_name=GCS_TEMP_BUCKET,
                object_name=generate_gcs_object_name(
                    dag_id, upload_task_id, f"{ch_db}_{ch_table}"
                ),
                queue="kubernetes",
            )

            insert_task = GCSToPostgresChunkedOperator(
                task_id=f"insert_from_gcs_{ch_db}_{ch_table}",
                postgres_conn_id=POSTGRES_SCORING_API_CONNECTION_ID,
                postgres_database="scoring_api",
                postgres_table=pg_table,
                gcp_conn_id=GOOGLE_CLOUD_CONNECTION_ID,
                bucket_name=GCS_TEMP_BUCKET,
                upload_task_id=upload_task_id,
                chunk_size=1024 * 256,
                on_conflict_index_columns=index_columns,
                method=GCS_TO_PG_METHOD_COPY_FROM_STDIN,
                queue="kubernetes",
            )

            (
                start
                >> dbt_operator
                >> create_replicated_wallets_set
                >> collect_replicated_addresses
                >> upload_task
                >> insert_task
                >> finish
            )

        if ch_db == CURRENT_SCHEMA_DBT:
            test_data_task = PythonOperator(
                task_id='test_all_attr',
                python_callable=test_all_attr,
                provide_context=True,
            )

            refresh_mv_all_wallets_task = PostgresOperator(
                task_id="refresh_mv_all_wallets",
                postgres_conn_id=POSTGRES_SCORING_API_CONNECTION_ID,
                sql="REFRESH MATERIALIZED VIEW mv__all_wallets",  # no need to use CONCURRENTLY here
            )
            refresh_mv_wallet_attributes_task = PostgresOperator(
                task_id="refresh_mv_wallet_attributes",
                postgres_conn_id=POSTGRES_SCORING_API_CONNECTION_ID,
                sql="mv__wallet_attributes.sql",
            )

            superdao_app_cache_invalidation_task = SimpleHttpOperator(
                task_id='invalidate_superdao_app_cache',
                http_conn_id=SUPERDAO_APP_API_CONN_ID,
                method='GET',
                endpoint=f"{SUPERDAO_APP_CACHE_INVALIDATION_ENDPOINT}",
                headers={"authorization": f"api_key {superdao_app_api_key}"},
                extra_options={"timeout": 600},
                response_check=lambda response: response.status_code == 200,
                log_response=True,
            )

            (
                finish
                >> test_data_task
                >> refresh_mv_all_wallets_task
                >> refresh_mv_wallet_attributes_task
                >> superdao_app_cache_invalidation_task
            )

    return replication_dag()


dbt_replication_dag = build_replication_dag(CURRENT_SCHEMA_DBT, None)
