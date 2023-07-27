from datetime import datetime, timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow_dbt.operators.dbt_operator import DbtTestOperator

from common.slack_notification import slack_fail_alert

default_args = {
    'start_date': datetime(2022, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'on_failure_callback': slack_fail_alert,
    # recommended for DBT
    'max_active_runs': 1,
    'profiles_dir': '/opt/airflow/secrets',
    'dir': '/opt/dbt_clickhouse',
}
dag_id = 'dbt_test_ethereum_etl'

with DAG(
    dag_id=dag_id,
    description='DAG for testing ETH and POLYGON data',
    start_date=days_ago(1),
    default_args=default_args,
    schedule=None,  #'@daily'
) as dag:
    operator_dbt_test_eth_data = DbtTestOperator(
        task_id='dbt_test_ethereum_etl_sources',
        retries=0,  # Fail with no retries if source is bad
        select='source:eth_data',
    )

    operator_dbt_test_polygon_data = DbtTestOperator(
        task_id='dbt_test_polygon_etl_sources',
        retries=0,  # Fail with no retries if source is bad
        select='source:polygon_data',
    )

    operator_dbt_test_ethereum_attributes = DbtTestOperator(
        task_id='dbt_test_ethereum_attributes',
        retries=0,
        select='source:attributes_to_psql',
        exclude='tag:attribute_updated',
    )

    operator_dbt_test_polygon_attributes = DbtTestOperator(
        task_id='dbt_test_polygon_attributes',
        retries=0,
        select='source:attributes_to_psql_polygon',
        exclude='tag:attribute_updated',
    )
