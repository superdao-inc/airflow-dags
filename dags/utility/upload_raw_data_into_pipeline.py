import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow_dbt.operators.dbt_operator import DbtRunOperator, DbtTestOperator

from common.chains.ethereum.ethereum_etl.functions import (
    generate_dictionary,
    insert_raw_data_pipeline,
    insert_raw_data_pipeline_old,
    truncate_buf_raw_data,
)
from common.connections import CLICKHOUSE_CONNECTION_ID, GOOGLE_CLOUD_CONNECTION_ID
from common.dags_config import create_dag
from common.gcs import (
    GCS_ETL_BUCKET,
    GCS_S3_COMPATIBLE_ENDPOINT,
    GCS_S3_COMPATIBLE_TEMP_BUCKET_ACCESS_KEY_ID,
    GCS_S3_COMPATIBLE_TEMP_BUCKET_SECRET,
)
from common.operators import S3ToClickhouseOperator
from common.slack_notification import slack_fail_alert

# resource_config = {"KubernetesExecutor": {"request_memory": "5000Mi", "request_cpu": "6000m"}}

dag = create_dag(
    dag_id='upload_raw_data_into_pipeline',
    schedule_interval=None,  #'0 0 * * *',
    on_failure_callback=slack_fail_alert,
    tags=['etl', 'ethereum', 'polygon'],
    start_date=datetime(2023, 4, 16),
    retries=2,
    retry_delay=timedelta(minutes=5),
    depends_on_past=False,
    dbt_project_dir='/opt/dbt_clickhouse',
    dbt_profiles_dir='/opt/airflow/secrets',
    execution_timeout=timedelta(minutes=300),
    dagrun_timeout=timedelta(minutes=300),
    sla=timedelta(minutes=300),
)


def load_data_from_s3_to_clickhouse(**kwargs):
    ti = kwargs['ti']
    chain = kwargs['chain']
    table = kwargs['table']
    task_name = kwargs['task_name']
    params = kwargs['params']

    if 'date' in params:
        print(params['date'])
        date = params['date'] if params['date'] else (datetime.now()).strftime("%m%d_")
    else:
        date = (datetime.now()).strftime("%m%d_")

    load_data_from_s3 = GCSListObjectsOperator(
        task_id=task_name + 'task',
        bucket='superdao-etl-raw-data',
        prefix=f'BQ/{chain}/{table}/{table}_2023{date}',
        delimiter='.json.gz',
        gcp_conn_id=GOOGLE_CLOUD_CONNECTION_ID,
    )
    files = load_data_from_s3.execute(context=kwargs)
    print(files)

    ti.xcom_push(key='return_value', value=files)


def insert_objects(**kwargs):
    ti = kwargs['ti']
    task_name = kwargs['task_name']
    chain = kwargs['chain']
    table = kwargs['table']
    objects = ti.xcom_pull(task_ids=task_name, key='return_value')
    print(objects)

    for i, obj in enumerate(objects, start=1):
        insert_gcs_objects = S3ToClickhouseOperator(
            task_id=f'insert_{i}',
            clickhouse_conn_id=CLICKHOUSE_CONNECTION_ID,
            clickhouse_database=f'{chain}_raw_data',
            clickhouse_table=f'buf_{table}',
            gcp_conn_id=GOOGLE_CLOUD_CONNECTION_ID,
            s3_access_key_id=GCS_S3_COMPATIBLE_TEMP_BUCKET_ACCESS_KEY_ID,
            s3_secret_key=GCS_S3_COMPATIBLE_TEMP_BUCKET_SECRET,
            s3_endpoint=GCS_S3_COMPATIBLE_ENDPOINT,
            s3_bucket_name='superdao-etl-raw-data',
            s3_object_name=obj,
            format='JSONEachRow',
        )
        insert_gcs_objects.execute(context=kwargs)


start = EmptyOperator(dag=dag, task_id="start", trigger_rule=TriggerRule.ALL_DONE)
finish = EmptyOperator(dag=dag, task_id="finish", trigger_rule=TriggerRule.ALL_DONE)

test_main_raw_data = DbtTestOperator(
    dag=dag,
    task_id=f'dbt_test_all_raw_data_main',
    retries=1,
    select=f'source:raw_data_main',
    trigger_rule=TriggerRule.ALL_SUCCESS,
)

generate_dictionary_task = PythonOperator(
    dag=dag,
    task_id=f'generate_dictionary',
    python_callable=generate_dictionary,
    provide_context=True,
)

for chain in ['ethereum', 'polygon']:
    for table in [
        'transactions',
        'blocks',
        'contracts',
        'tokens',
        'token_transfers',
        'logs',
    ]:
        truncate_buf_table = PythonOperator(
            dag=dag,
            task_id=f'truncate_buf_{chain}_{table}',
            python_callable=truncate_buf_raw_data,
            op_kwargs={"table": table, "chain": chain},
            provide_context=True,
        )

        task_name = f'gcs_bucket_objects_list_{chain}_{table}'
        gcs_bucket_objects_list = PythonOperator(
            dag=dag,
            task_id=task_name,
            python_callable=load_data_from_s3_to_clickhouse,
            op_kwargs={"table": table, "chain": chain, "task_name": task_name},
            provide_context=True,
        )

        insert_objects_task = PythonOperator(
            dag=dag,
            task_id=f'insert_objects_{chain}_{table}',
            python_callable=insert_objects,
            op_kwargs={"table": table, "chain": chain, "task_name": task_name},
            provide_context=True,
        )

        test_buf_raw_data = DbtTestOperator(
            dag=dag,
            task_id=f'dbt_test_all_raw_data_{chain}_{table}',
            retries=1,
            select=f'source:raw_data_{chain}.{table}',
            trigger_rule=TriggerRule.ALL_SUCCESS,
        )

        insert_data_to_raw = PythonOperator(
            dag=dag,
            task_id=f'insert_data_to_raw_{chain}_{table}',
            python_callable=insert_raw_data_pipeline,
            op_kwargs={"table": table, "chain": chain},
            provide_context=True,
        )

        insert_data_to_old_raw = PythonOperator(
            dag=dag,
            task_id=f'insert_data_to_raw_{chain}_{table}_old',
            python_callable=insert_raw_data_pipeline_old,
            op_kwargs={"table": table, "chain": chain},
            provide_context=True,
        )

        (
            start
            >> truncate_buf_table
            >> gcs_bucket_objects_list
            >> insert_objects_task
            >> test_buf_raw_data
            >> [insert_data_to_raw, insert_data_to_old_raw]
            >> test_main_raw_data
            >> generate_dictionary_task
            >> finish
        )
