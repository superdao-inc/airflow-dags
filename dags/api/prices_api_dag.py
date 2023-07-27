from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow_dbt.operators import DbtRunOperator, DbtTestOperator

from common.api.binance import getters, loaders


def dag_load_coins():
    l = getters.get_all_prices()
    loaders.load_symbols(l)


default_args = {
    'start_date': datetime(2022, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'catchup': False,
    # recommended for DBT
    'max_active_runs': 1,
    'profiles_dir': '/opt/airflow/secrets',
    'dir': '/opt/dbt_clickhouse',
}

dag_id = 'prices'

with DAG(
    dag_id=dag_id,
    description='DAG to load coingecko prices',
    start_date=days_ago(1),
    default_args=default_args,
    schedule='@hourly',
) as dag:
    operator_load_binance = PythonOperator(
        task_id='load_simple_binance', python_callable=dag_load_coins
    )

    operator_dbt_test_prices = DbtTestOperator(
        task_id='dbt_test_prices', select='source:prices'
    )

    operator_dbt_run_prices = DbtRunOperator(
        task_id='dbt_run_prices', select='last_prices'
    )

    operator_dbt_aftertest_prices = DbtTestOperator(
        task_id='dbt_aftertest_prices', select='prices'
    )

    (
        operator_load_binance
        >> operator_dbt_test_prices
        >> operator_dbt_run_prices
        >> operator_dbt_aftertest_prices
    )
