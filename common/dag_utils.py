import re
from typing import Tuple

from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from airflow_dbt import DbtRunOperator, DbtTestOperator

from common.connections import CLICKHOUSE_CONNECTION_ID as clickhouse_conn
from common.connections import GOOGLE_CLOUD_CONNECTION_ID as gcloud_conn
from common.connections import POSTGRES_SCORING_API_CONNECTION_ID as ps_conn
from common.gcs import GCS_TEMP_BUCKET, generate_gcs_object_name
from common.operators import (
    GCS_TO_PG_METHOD_COPY_FROM_STDIN,
    ClickhouseToGCSOperator,
    GCSToPostgresChunkedOperator,
)

hex_letters = [
    '0x0',
    '0x1',
    '0x2',
    '0x3',
    '0x4',
    '0x5',
    '0x6',
    '0x7',
    '0x8',
    '0x9',
    '0xa',
    '0xb',
    '0xc',
    '0xd',
    '0xe',
    '0xf',
]

buckets = [
    ('1', '2', '3', '4'),
    ('5', '6', '7', '8'),
    ('9', 'a', 'b', 'c'),
    ('d', 'e', 'f', '0'),
]


BUFFER_PREFIX_TABLE = 'buffer_'
TMP_TABLE_PREFIX = 'tmp_'


def address_buckets_64():
    a = []

    for h in hex_letters:
        for b in buckets:
            a.append([(h + bb) for bb in b])

    return a


def dbt_run_and_test_tasks_generator(
    start_task,
    finish_task,
    dbt_select,
    **kwargs,
):
    exclude_str = kwargs.get('exclude')
    task_id = kwargs.get('task_id', 'dbt_task_' + re.sub('[.|,|:| ]', '-', dbt_select))
    pool = kwargs.get('pool', 'default_pool')

    with TaskGroup(group_id=task_id) as tg:
        run = DbtRunOperator(
            task_id=task_id + '_RUN', select=dbt_select, exclude=exclude_str, pool=pool
        )
        test = DbtTestOperator(
            task_id=task_id + '_TEST', select=dbt_select, exclude=exclude_str, pool=pool
        )
        start_task >> run >> test >> finish_task


def dbt_runs_on_prefix(
    start_task,
    finish_task,
    dbt_select,
    mode='plain',  # plain or buckets
    **kwargs,
):
    task_id = kwargs.get('task_id', 'dbt_task_' + re.sub('[.|,|:]', '-', dbt_select))
    pool = kwargs.get('pool', 'default_pool')

    with TaskGroup(group_id=f'dbt_group_{task_id}') as tg:
        if mode == 'plain':
            for prefix in hex_letters:
                run = DbtRunOperator(
                    task_id=task_id + f'_{prefix}_RUN',
                    select=dbt_select,
                    vars=dict(token_buckets=[prefix]),
                    pool=pool,
                )

        elif mode == 'buckets':
            for i, bucket in enumerate(address_buckets_64()):
                run = DbtRunOperator(
                    task_id=task_id + f'_bucket_{i}_RUN',
                    select=dbt_select,
                    vars=dict(token_buckets=bucket),
                    pool=pool,
                )

        start_task >> tg >> finish_task


def clh2pg_tasks_generator(start_task, finish_task, table_data, **kwargs):
    ch_db = table_data['ch_db']
    ch_table = table_data['ch_table']
    pg_table = table_data['pg_table']
    pg_schema = table_data.get('pg_schema')
    truncate = kwargs.get('truncate', True)
    where_filters = kwargs.get('where_filters', [])
    distinct_on = kwargs.get('distinct_on', None)

    dag_id = kwargs.get('dag_id')
    upload_task_id = kwargs.get('upload_task_id', f'upload_to_gsc_{ch_db}_{ch_table}')
    insert_task_id = kwargs.get('insert_task_id', f'insert_from_gcs_{ch_db}_{ch_table}')

    distinct_statement = (
        f"DISTINCT ON ({','.join(distinct_on)})" if distinct_on is not None else ''
    )

    where_statement = ''
    for where_filter in where_filters:
        where_statement += f" AND {where_filter} "

    default_query = f'''
        SELECT {distinct_statement}
        *
        FROM {ch_db}.{ch_table}
    '''

    sql = kwargs.get('query', None)
    sql = default_query if sql is None else sql

    if where_statement != '':
        sql += f' WHERE 1=1 {where_statement}'

    pool = kwargs.get('pool', 'default_pool')
    task_group = kwargs.get('task_group')

    upload_task = ClickhouseToGCSOperator(
        task_id=upload_task_id,
        sql=sql,
        clickhouse_conn_id=clickhouse_conn,
        clickhouse_database=ch_db,
        gcp_conn_id=gcloud_conn,
        bucket_name=GCS_TEMP_BUCKET,
        object_name=generate_gcs_object_name(
            dag_id, upload_task_id, f'{ch_db}_{ch_table}'
        ),
        queue='kubernetes',
        pool=pool,
        task_group=task_group,
    )

    insert_task = GCSToPostgresChunkedOperator(
        task_id=insert_task_id,
        postgres_conn_id=ps_conn,
        postgres_database='scoring_api',
        postgres_schema=pg_schema,
        postgres_table=pg_table,
        truncate=truncate,
        gcp_conn_id=gcloud_conn,
        bucket_name=GCS_TEMP_BUCKET,
        upload_task_id=f"{task_group.group_id}.{upload_task_id}"
        if task_group is not None
        else upload_task_id,
        chunk_size=1024 * 256,
        queue='kubernetes',
        method=GCS_TO_PG_METHOD_COPY_FROM_STDIN,
        pool=pool,
        task_group=task_group,
    )

    start_task.set_downstream(upload_task)
    upload_task >> insert_task
    finish_task.set_upstream(insert_task)


def ch2pg_parallel_by_wallet_prefix_task_generator(
    start_task, finish_task, hex_column, table_data, **kwargs
):
    '''
    Parallel loader by 0x_wallet prefix.

    sql_where template example:
        wallet like '{prefix}'
    '''

    pool = kwargs.get('pool')
    dag_id = kwargs.get('dag_id')

    # for task naming only
    ch_db = table_data['ch_db']
    ch_table = table_data['ch_table']
    distinct_on = kwargs.get('distinct_on', None)
    where_filters = kwargs.get('where_filters', [])
    query = kwargs.get('query', None)
    tg_prefix = kwargs.get('task_group_prefix', None)

    tg_id = f'tg_upload_{ch_db}_{ch_table}'
    if tg_prefix:
        tg_id += f'_{tg_prefix}'

    with TaskGroup(group_id=tg_id) as tg:
        for hex in hex_letters:
            where = where_filters + [f"startsWith({hex_column}, '{hex}')"]

            upload_task_id = f'upload_to_gsc_{ch_db}_{ch_table}_{hex}'
            insert_task_id = f'insert_from_gcs_{ch_db}_{ch_table}_{hex}'

            clh2pg_tasks_generator(
                start_task=start_task,
                finish_task=finish_task,
                table_data=table_data,
                query=query,
                where_filters=where,
                distinct_on=distinct_on,
                truncate=False,
                dag_id=dag_id,
                pool=pool,
                upload_task_id=upload_task_id,
                insert_task_id=insert_task_id,
                task_group=tg,
            )


def swap_tables_operator(
    conn_id,
    original_table,
    owner_name='scoring_api',
):
    tmp_table = TMP_TABLE_PREFIX + original_table

    return PostgresOperator(
        task_id=f'swap_table_{tmp_table}',
        postgres_conn_id=conn_id,
        sql=f'''
            BEGIN;
            DROP TABLE IF EXISTS {original_table};
            ALTER TABLE {tmp_table} RENAME TO {original_table};
            ALTER TABLE {original_table} OWNER TO {owner_name};
            COMMIT;
            ''',
    )


def create_tmp_table_operator(conn_id, original_table) -> Tuple[str, PostgresOperator]:
    '''
    Create table with the same structure as original table and with same indices.
    Used for data loading via following pattern:
    INSERT INTO tmp_table; DROP original_table; ALTER TABLE tmp_table RENAME TO original_table;
    '''
    tmp_table = TMP_TABLE_PREFIX + original_table

    return (
        tmp_table,
        PostgresOperator(
            task_id=f'create_tmp_table_{tmp_table}',
            postgres_conn_id=conn_id,
            sql=f'''
                CREATE TABLE IF NOT EXISTS {tmp_table}
                (LIKE {original_table} INCLUDING ALL);
                TRUNCATE TABLE {tmp_table};
                ''',
        ),
    )


def create_buffer_table_operator(
    conn_id, original_table
) -> Tuple[str, PostgresOperator]:
    '''
    Create table with the same structure as original table but without any indices.
    Used for data loading via following pattern:
    DELETE FROM original_table; INSERT INTO original_table SELECT * FROM buffer_table;
    '''
    tmp_table = BUFFER_PREFIX_TABLE + original_table

    return (
        tmp_table,
        PostgresOperator(
            task_id=f'create_buffer_table_{tmp_table}',
            postgres_conn_id=conn_id,
            sql=f'''
                CREATE TABLE IF NOT EXISTS {tmp_table}
                AS TABLE {original_table}
                WITH NO DATA;
                TRUNCATE TABLE {tmp_table};
                ''',
        ),
    )


def drop_table_operator(conn_id, table_name):
    return PostgresOperator(
        task_id=f'drop_table_{table_name}',
        postgres_conn_id=conn_id,
        sql=f'''
            DROP TABLE IF EXISTS {table_name};
            ''',
    )
