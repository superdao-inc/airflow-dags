from datetime import datetime, timedelta

from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator
from airflow_dbt.operators.dbt_operator import DbtRunOperator

from common.connections import CLICKHOUSE_CONNECTION_ID
from common.dags_config import create_dag
from common.environment import CURRENT_SCHEMA_DBT
from common.slack_notification import slack_fail_alert


def get_delete_claimed_audiences_v2_sql(ch_database):
    return f"""
        ALTER TABLE {ch_database}.claimed_audiences_v2 DELETE
        WHERE tokenAddress IN (
            SELECT collection_contract FROM {ch_database}.input_claimed_audiences
        )
    """


create_claimed_audience_task_id = "create_claimed_audience"
dag_task_params = [
    {
        "ch_database": "eth_data",
        "is_fungible": "false",
    },
    {
        "ch_database": "eth_data",
        "is_fungible": "true",
    },
    {
        "ch_database": "polygon_data",
        "is_fungible": "false",
    },
    {
        "ch_database": "polygon_data",
        "is_fungible": "true",
    },
]

default_args = {
    "start_date": datetime(2023, 4, 24),
}

dag = create_dag(
    dag_id="rebuild_claimed_audiences",
    schedule_interval="0 23 * * *",
    description="Build all claimed audiences",
    catchup=False,
    start_date=days_ago(2),
    max_active_runs=1,
    retry_delay=timedelta(minutes=10),
    on_failure_callback=slack_fail_alert,
)

start = EmptyOperator(task_id="start", trigger_rule=TriggerRule.ALL_DONE, dag=dag)
finish = EmptyOperator(task_id="finish", trigger_rule=TriggerRule.ALL_DONE, dag=dag)

delete_tasks = []
for ch_db in ["eth_data", "polygon_data"]:
    delete_sql = get_delete_claimed_audiences_v2_sql(ch_db)
    delete_task = ClickHouseOperator(
        task_id=f"delete_claimed_audiences_v2_{ch_db}",
        clickhouse_conn_id=CLICKHOUSE_CONNECTION_ID,
        sql=delete_sql,
        dag=dag,
    )
    delete_tasks.append(delete_task)

tasks = []
for task_params in dag_task_params:
    """
    Create a task for each combination of ch_database and is_fungible.
    tasks flow: start -> [dbt_run -> clickhouse, dbt_run -> clickhouse,...] -> finish
    """
    ch_database = task_params["ch_database"]
    is_fungible = task_params["is_fungible"]
    is_fungible_str = "fungible" if is_fungible == "true" else "nonfungible"

    dbt_operator = DbtRunOperator(
        vars={
            "ch_database": ch_database,
            "is_fungible": is_fungible,
        },
        task_id=f"dbt_run_{ch_database}_{is_fungible_str}_claimed_audiences",
        models="all_claimed_audiences",
        dag=dag,
    )

    insert_sql = f"""
        INSERT INTO {ch_database}.claimed_audiences_v2 
        SELECT * FROM {CURRENT_SCHEMA_DBT}.all_claimed_audiences
    """
    ch_operator = ClickHouseOperator(
        task_id=f"clickhouse_{ch_database}_{is_fungible_str}_claimed_audiences",
        clickhouse_conn_id=CLICKHOUSE_CONNECTION_ID,
        sql=insert_sql,
        dag=dag,
    )

    tasks.append(dbt_operator)
    tasks.append(ch_operator)


chain(start, *delete_tasks, *tasks, finish)
