import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow_dbt.operators.dbt_operator import DbtRunOperator, DbtTestOperator

from common.dags_config import create_dag
from common.slack_notification import slack_fail_alert

args = {
    "start_date": datetime(2022, 1, 1),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
    "on_failure_callback": slack_fail_alert,
}


test_slack = dag = create_dag(
    dag_id="test_slack",
    schedule_interval="0 23 * * *",
    description="",
    catchup=False,
    start_date=days_ago(2),
    max_active_runs=1,
    retry_delay=timedelta(minutes=10),
    on_failure_callback=slack_fail_alert,
    dbt_profiles_dir=None,
    dbt_project_dir=None,
)

with test_slack:

    @task()
    def test_slack_task():
        logging.info("Sending slack message")
        raise Exception("Test exception")

    token_exclude_str = "tag:wallet_tokens tag:for_audiences"
    dbt_run_wallet_tokens = DbtRunOperator(
        task_id=f"dbt_run__wallet_tokens_exclude_audiences",
        select=f"tokens",
        exclude=token_exclude_str,
    )

    dbt_run_wallet_tokens >> test_slack_task()


# test_slack.test()
