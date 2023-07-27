from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator

from common.connections import CLICKHOUSE_CONNECTION_ID as clickhouse_conn
from common.dag_utils import dbt_runs_on_prefix
from common.environment import CURRENT_SCHEMA_DBT

args = {
    'start_date': datetime(2022, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'tags': ['utility', 'dbt'],
    # recommended for DBT
    'max_active_runs': 1,
    'profiles_dir': '/opt/airflow/secrets',
    'dir': '/opt/dbt_clickhouse',
}

DAG_ID = 'erc_1155_dag'

with DAG(
    dag_id=DAG_ID,
    default_args=args,
    schedule=None,
) as dag:
    done_holders = EmptyOperator(task_id='done_holders')
    done_wallet_tokens = EmptyOperator(task_id='done_wallet_tokens')

    truncate_holders = ClickHouseOperator(
        clickhouse_conn_id=clickhouse_conn,
        task_id=f'truncate_holders',
        sql='TRUNCATE TABLE IF EXISTS {schema}.{model}'.format(
            schema=CURRENT_SCHEMA_DBT,
            model='polygon_token_holders_erc_1155',
        ),
    )

    dbt_runs_on_prefix(
        start_task=truncate_holders,
        finish_task=done_holders,
        dbt_select='polygon_token_holders_erc_1155',
        mode='plain',
        pool='dbt_single',
    )

    truncate_tokens = ClickHouseOperator(
        clickhouse_conn_id=clickhouse_conn,
        task_id=f'truncate_tokens',
        sql='TRUNCATE TABLE IF EXISTS {schema}.{model}'.format(
            schema=CURRENT_SCHEMA_DBT,
            model='polygon_wallet_tokens_erc_1155',
        ),
    )
    done_holders >> truncate_tokens

    dbt_runs_on_prefix(
        start_task=truncate_tokens,
        finish_task=done_wallet_tokens,
        dbt_select='polygon_wallet_tokens_erc_1155',
        mode='plain',
        pool='dbt_single',
    )
