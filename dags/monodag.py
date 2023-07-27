from datetime import timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator
from airflow_dbt.operators.dbt_operator import DbtRunOperator, DbtTestOperator

from common.connections import CLICKHOUSE_CONNECTION_ID as clickhouse_conn
from common.connections import GOOGLE_CLOUD_CONNECTION_ID as gcloud_conn
from common.connections import POSTGRES_SCORING_API_CONNECTION_ID as ps_conn
from common.dag_utils import (
    clh2pg_tasks_generator,
    create_buffer_table_operator,
    dbt_run_and_test_tasks_generator,
    dbt_runs_on_prefix,
    drop_table_operator,
)
from common.environment import CURRENT_SCHEMA_DBT, CURRENT_SCHEMA_SCORING_API
from common.gcs import GCS_TEMP_BUCKET, generate_gcs_object_name
from common.operators import GCSToClickhouseOperator, PostgresToGCSOperator

default_args = {
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
    'depends_on_past': False,
    'catchup': False,
    'max_active_runs': 1,
    'profiles_dir': '/opt/airflow/secrets',
    'dir': '/opt/dbt_clickhouse',
}

DAG_ID = 'v3_monodag'
POSTGRES_INDEX_COLUMNS = ["wallet", "claimed_contract"]
PG_CLAIMED_TABLE_NAME = 'claimed'


chain_list = {'eth_data': 'eth', 'polygon_data': 'polygon'}

# CSV AUDs
psql_table = dict(src_table='fixed_lists', src_db='scoring_api')
clickhouse_table = dict(database='off_chain', table='scoring_api_fixed_lists')

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    # FROM CONTROL DAG ONLY
    schedule=None,
    catchup=False,
) as dag:
    start = EmptyOperator(task_id='all_start')
    done_attrs_prereq = EmptyOperator(task_id='done_attributes_prerequisites')
    done_attrs_calc = EmptyOperator(task_id='done_attrs_calculation')
    start_audience_prereq = EmptyOperator(task_id='start_audiences_prerequisites')
    done_audience_prereq = EmptyOperator(task_id='done_audiences_prerequisites')
    done_models_for_audiences = EmptyOperator(task_id='done_models_for_audiences')
    start_audiences_replication = EmptyOperator(task_id='start_audiences_replication')
    done_audiences_replication = EmptyOperator(task_id='done_audiences_replication')
    all_finish = EmptyOperator(task_id='all_finish')

    start >> start_audience_prereq

    # DBT prerequisite models
    token_exclude_str = 'tag:wallet_tokens tag:for_audiences'
    dbt_run_wallet_tokens = DbtRunOperator(
        task_id=f'dbt_run__wallet_tokens_exclude_audiences',
        select=f'tokens',
        exclude=token_exclude_str,
    )
    dbt_run_token_holders_all = DbtRunOperator(
        task_id=f'dbt_run__token_holders_all',
        select=f'tag:token_holders_all',
    )
    dbt_test_wallet_tokens = DbtTestOperator(
        task_id=f'dbt_test__wallet_tokens_exclude_audiences',
        select=f'tokens',
        exclude='tag:for_audiences',
    )

    dbt_run_and_test_tasks_generator(
        start_task=dbt_test_wallet_tokens,
        finish_task=done_attrs_prereq,
        dbt_select='transactions',
        dag_id=DAG_ID,
    )

    # Tokens and transactions data
    for chain in chain_list.values():
        # Test general sources
        test_source = DbtTestOperator(
            task_id=f'dbt_test_{chain}_sources',
            retries=0,  # Fail with no retries if source is bad
            select=f'source:{chain}_data',
        )

        model_name = f'{chain}_wallet_tokens'
        truncate_wallet_tokens = ClickHouseOperator(
            clickhouse_conn_id=clickhouse_conn,
            task_id=f'truncate_{chain}_wallet_tokens',
            sql=f'TRUNCATE TABLE IF EXISTS {CURRENT_SCHEMA_DBT}.{model_name}',
        )

        start >> test_source >> dbt_run_wallet_tokens >> truncate_wallet_tokens

        dbt_runs_on_prefix(
            truncate_wallet_tokens,
            dbt_run_token_holders_all,
            model_name,
            'plain',
            pool='dbt_multi',
        )

        dbt_run_token_holders_all >> dbt_test_wallet_tokens

    dbt_test_wallet_tokens >> done_models_for_audiences

    # Building audiences
    operator_extract_fixed_lists = PostgresToGCSOperator(
        task_id='extract_fixed_audiences',
        sql='SELECT * FROM {src_table}'.format(src_table=psql_table['src_table']),
        postgres_conn_id=ps_conn,
        postgres_database=psql_table['src_db'],
        gcp_conn_id=gcloud_conn,
        bucket_name=GCS_TEMP_BUCKET,
        object_name=generate_gcs_object_name(
            dag_id=DAG_ID,
            task_id='extract_fixed_audiences',
            name=psql_table['src_table'],
        ),
    )
    operator_insert_fixed_lists = GCSToClickhouseOperator(
        task_id='insert_fixed_lists',
        clickhouse_conn_id=clickhouse_conn,
        clickhouse_database=clickhouse_table['database'],
        clickhouse_table=clickhouse_table['table'],
        gcp_conn_id=gcloud_conn,
        bucket_name=GCS_TEMP_BUCKET,
        upload_task_id='extract_fixed_audiences',
        columns_mapper=dict(),
    )
    dbt_test_fixed_lists = DbtTestOperator(
        task_id='dbt_test_fixed_lists',
        select='source:audiences.scoring_api_fixed_lists',
    )

    dbt_run_all_audiences = DbtRunOperator(
        task_id='dbt_run_all_audiences', select='all_audiences'
    )
    dbt_test_all_audiences = DbtRunOperator(
        task_id='dbt_test_all_audiences', select='all_audiences'
    )

    (
        done_audience_prereq
        >> dbt_run_all_audiences
        >> dbt_test_all_audiences
        >> start_audiences_replication
    )

    # Replicate claimed audiences:

    with TaskGroup(group_id='replicate_claimed_audiences') as tg:
        [
            claimed_buffer_table,
            create_tmp_claimed_table_task,
        ] = create_buffer_table_operator(ps_conn, PG_CLAIMED_TABLE_NAME)

        start_audiences_replication >> create_tmp_claimed_table_task

        update_claimed_prod = PostgresOperator(
            task_id='update_prod_claimed_audiences',
            postgres_conn_id=ps_conn,
            sql=f'''
                DELETE FROM {PG_CLAIMED_TABLE_NAME};
                INSERT INTO {PG_CLAIMED_TABLE_NAME} SELECT * FROM {claimed_buffer_table};
                ''',
        )

        select_sql = f'''
                    SELECT DISTINCT ON (token_address, address, chain)
                        address as wallet,
                        if(chain = 'eth', 'ETHEREUM', 'POLYGON') as blockchain,
                        token_address as claimed_contract,
                        concat('\\\\x', upper(substring(wallet, 3))) as wallet_b
                    FROM {CURRENT_SCHEMA_DBT}.all_claimed_audiences
            '''

        clh2pg_tasks_generator(
            start_task=create_tmp_claimed_table_task,
            finish_task=update_claimed_prod,
            table_data={
                'ch_db': CURRENT_SCHEMA_DBT,
                'pg_schema': CURRENT_SCHEMA_SCORING_API,
                'ch_table': 'all_claimed_audiences',
                'pg_table': claimed_buffer_table,
            },
            query=select_sql,
            truncate=False,
            dag_id=DAG_ID,
            task_group=tg,
        )

        drop_buffer_table = drop_table_operator(
            conn_id=ps_conn, table_name=claimed_buffer_table
        )

        (update_claimed_prod >> drop_buffer_table >> done_audiences_replication)

    # ALL CLAIMED AUDIENCES CALC
    dbt_run_and_test_tasks_generator(
        start_task=done_models_for_audiences,
        finish_task=dbt_run_all_audiences,
        dbt_select='audiences.claimed_audiences',
    )

    # Build attributes
    dbt_run_and_test_tasks_generator(
        start_task=done_attrs_prereq,
        finish_task=done_attrs_calc,
        dbt_select='attributes',
        exclude='tag:attribute_updated',
        dag_id=DAG_ID,
    )

    (
        start_audience_prereq
        >> operator_extract_fixed_lists
        >> operator_insert_fixed_lists
        >> dbt_test_fixed_lists
        >> done_audience_prereq
    )

    (done_audiences_replication, done_attrs_calc) >> all_finish
