from datetime import timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator

from common.connections import (
    CLICKHOUSE_CONNECTION_ID,
    POSTGRES_SCORING_API_CONNECTION_ID,
)
from common.dag_utils import (
    ch2pg_parallel_by_wallet_prefix_task_generator,
    clh2pg_tasks_generator,
    create_tmp_table_operator,
    dbt_run_and_test_tasks_generator,
    dbt_runs_on_prefix,
    swap_tables_operator,
)
from common.environment import CURRENT_SCHEMA_DBT

POOL_NAME__COMPUTING = 'dbt_top_collections_pool'
POOL_NAME__REPLICATION = 'clh_to_pg_pool'

DAG_ID = 'top_collections_revisited'
TOP_NOTABLE_FINAL_MODEL = 'all_audiences_top_notable_collections'
TOP_ALL_FINAL_MODEL = 'all_audiences_top_nft_collections'

PG_TOP_NFT_TABLE_NAME = 'top_collections'
PG_TOP_NOTABLE_TABLE_NAME = 'top_whitelisted_collections'


default_args = {
    'start_date': days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
    'depends_on_past': False,
    'catchup': False,
    'max_active_runs': 1,
    'profiles_dir': '/opt/airflow/secrets',
    'dir': '/opt/dbt_clickhouse',
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule=None,
    catchup=False,
) as dag:
    start = EmptyOperator(task_id='start')
    done_prepare_wallet_tokens = EmptyOperator(task_id='done_prepare_wallet_tokens')
    done_compute_top_25 = EmptyOperator(task_id='done_compute_top_25')
    done_compute_all_models = EmptyOperator(task_id='done_compute_all_models')
    finish = EmptyOperator(task_id='finish')

    dbt_run_and_test_tasks_generator(
        start_task=start,
        finish_task=done_prepare_wallet_tokens,
        dbt_select='tag:top_collections_prerequisites',
    )

    # 1. Compute top 25 collections
    top_25_settings = {
        'eth': {'model': 'eth_top_25_claimed_chunks', 'mode': 'buckets'},
        'polygon': {'model': 'polygon_top_25_claimed_chunks', 'mode': 'buckets'},
    }
    for chain, chain_data in top_25_settings.items():
        top_25_model = chain_data['model']
        mode = chain_data['mode']

        truncate_top_25_ch = ClickHouseOperator(
            clickhouse_conn_id=CLICKHOUSE_CONNECTION_ID,
            task_id=f'truncate_{chain}_top25_chunks',
            sql=f'TRUNCATE TABLE IF EXISTS {CURRENT_SCHEMA_DBT}.{top_25_model}',
        )

        done_prepare_wallet_tokens >> truncate_top_25_ch

        dbt_runs_on_prefix(
            start_task=truncate_top_25_ch,
            finish_task=done_compute_top_25,
            dbt_select=top_25_model,
            mode=mode,
            pool=POOL_NAME__COMPUTING,
        )

    # 2. Compute all models for top collections
    dbt_run_and_test_tasks_generator(
        start_task=done_compute_top_25,
        finish_task=done_compute_all_models,
        dbt_select='audiences.top_collections',
        exclude='tag:chunks',  # important
        dag_id=DAG_ID,
    )

    # 3. Send it all to postgres
    [tmp_notable_table, create_tmp_notable_table] = create_tmp_table_operator(
        conn_id=POSTGRES_SCORING_API_CONNECTION_ID,
        original_table=PG_TOP_NOTABLE_TABLE_NAME,
    )

    [tmp_all_table, create_tmp_all_table] = create_tmp_table_operator(
        conn_id=POSTGRES_SCORING_API_CONNECTION_ID,
        original_table=PG_TOP_NFT_TABLE_NAME,
    )

    swap_top_all_table = swap_tables_operator(
        conn_id=POSTGRES_SCORING_API_CONNECTION_ID,
        original_table=PG_TOP_NFT_TABLE_NAME,
    )

    swap_top_notable_table = swap_tables_operator(
        conn_id=POSTGRES_SCORING_API_CONNECTION_ID,
        original_table=PG_TOP_NOTABLE_TABLE_NAME,
    )

    done_compute_all_models >> (create_tmp_notable_table, create_tmp_all_table)
    (swap_top_all_table, swap_top_notable_table) >> finish

    replication_settings = [
        {
            'start_task': create_tmp_notable_table,
            'finish_task': swap_top_notable_table,
            'src_table': TOP_NOTABLE_FINAL_MODEL,
            'target_table': tmp_notable_table,
            'distinct_columns': [
                'chain',
                'audience_slug',
                'audience_type',
                'token_address',
            ],
        },
        {
            'start_task': create_tmp_all_table,
            'finish_task': swap_top_all_table,
            'src_table': TOP_ALL_FINAL_MODEL,
            'target_table': tmp_all_table,
            'distinct_columns': [
                'chain',
                'audience_slug',
                'audience_type',
                'token_address',
            ],
            'filters': ["audience_type <> 'CLAIMED'"],
        },
        {
            'start_task': create_tmp_all_table,
            'finish_task': swap_top_all_table,
            'src_table': TOP_ALL_FINAL_MODEL,
            'target_table': tmp_all_table,
            'distinct_columns': [
                'chain',
                'audience_slug',
                'audience_type',
                'token_address',
            ],
            'filters': ["audience_type = 'CLAIMED'"],
            'partitions_by': 'audience_slug',
        },
    ]

    for settings in replication_settings:
        start_task = settings['start_task']
        finish_task = settings['finish_task']
        src_table = settings['src_table']
        target_table = settings['target_table']
        distinct_columns = settings.get('distinct_columns', None)
        filters = settings.get('filters', [])

        partitions_by = settings.get('partitions_by', None)

        tables_data = {
            'ch_db': CURRENT_SCHEMA_DBT,
            'ch_table': src_table,
            'pg_schema': 'public',
            'pg_table': target_table,
        }

        replication_method = clh2pg_tasks_generator
        if partitions_by:
            replication_method = ch2pg_parallel_by_wallet_prefix_task_generator

        replication_method(
            start_task=start_task,
            finish_task=finish_task,
            table_data=tables_data,
            distinct_on=distinct_columns,
            where_filters=filters,
            hex_column=partitions_by,
            pool=POOL_NAME__REPLICATION,
            dag_id=DAG_ID,
            truncate=False,
        )
