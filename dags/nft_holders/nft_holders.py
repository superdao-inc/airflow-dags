from datetime import timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

from common.connections import POSTGRES_SCORING_API_CONNECTION_ID
from common.dag_utils import ch2pg_parallel_by_wallet_prefix_task_generator
from common.environment import CURRENT_SCHEMA_DBT, CURRENT_SCHEMA_SCORING_API

default_args = {
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
    'depends_on_past': False,
    'catchup': False,
    'max_active_runs': 1,
}

DAG_ID = 'nft_holders_replication'

nft_targets = {
    'eth': {
        'ch_db': CURRENT_SCHEMA_DBT,
        'ch_table': 'token_holders_all',
        'pg_table': 'eth_nft_holders',
        'pg_schema': CURRENT_SCHEMA_SCORING_API,
        'distinct_columns': ['token_address', 'wallet'],
        'where_filters': ["chain = 'eth'"],
    },
    'polygon': {
        'ch_db': CURRENT_SCHEMA_DBT,
        'ch_table': 'token_holders_all',
        'pg_table': 'polygon_nft_holders',
        'pg_schema': CURRENT_SCHEMA_SCORING_API,
        'distinct_columns': ['token_address', 'wallet'],
        'where_filters': ["chain = 'polygon'"],
    },
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule=None,
    catchup=False,
) as dag:
    start = EmptyOperator(task_id='all_start')
    finish = EmptyOperator(task_id='all_finish')

    update_pg_table = PostgresOperator(
        task_id='update_prod_nft_holders',
        postgres_conn_id=POSTGRES_SCORING_API_CONNECTION_ID,
        sql=f'''DELETE FROM {CURRENT_SCHEMA_SCORING_API}.nft_holders;
                INSERT INTO {CURRENT_SCHEMA_SCORING_API}.nft_holders (
                    contract_prefix,
                    token_contract,
                    wallet,
                    wallet_b
                )
                SELECT DISTINCT
                    contract_prefix,
                    token_contract,
                    wallet,
                    wallet_b
                FROM (
                    SELECT
                        substring(token_contract,1,3) as contract_prefix,
                        token_contract,
                        wallet,
                        concat('\\\\x', upper(substring(wallet, 3))) as wallet_b
                    FROM {CURRENT_SCHEMA_SCORING_API}.eth_nft_holders

                    UNION ALL

                    SELECT
                        substring(token_contract,1,3) as contract_prefix,
                        token_contract,
                        wallet,
                        concat('\\\\x', upper(substring(wallet, 3))) as wallet_b
                    FROM {CURRENT_SCHEMA_SCORING_API}.polygon_nft_holders
                ) u
            ''',
        pool='clh_to_pg_pool',
    )

    vacuum_pg_table = PostgresOperator(
        task_id='vacuum_nft_holders',
        postgres_conn_id=POSTGRES_SCORING_API_CONNECTION_ID,
        sql=f'VACUUM {CURRENT_SCHEMA_SCORING_API}.nft_holders',
        autocommit=True,
        pool='clh_to_pg_pool',
    )

    update_pg_table >> vacuum_pg_table >> finish

    for db, target_params in nft_targets.items():
        target_table = target_params['pg_table']
        target_schema = target_params['pg_schema']
        ch_db = target_params['ch_db']
        ch_table = target_params['ch_table']

        truncate_pg_table = PostgresOperator(
            task_id=f'truncate_{target_table}',
            postgres_conn_id=POSTGRES_SCORING_API_CONNECTION_ID,
            sql=f'TRUNCATE TABLE {target_schema}.{target_table}',
            pool='clh_to_pg_pool',
        )

        start >> truncate_pg_table

        ch2pg_parallel_by_wallet_prefix_task_generator(
            start_task=truncate_pg_table,
            finish_task=update_pg_table,
            table_data=target_params,
            truncate=False,
            hex_column='wallet',
            pool='clh_to_pg_pool',
            dag_id=DAG_ID,
            task_group_prefix=db,
        )
