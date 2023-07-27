from datetime import timedelta

from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow_dbt.operators.dbt_operator import DbtRunOperator, DbtTestOperator

from common.dags_config import create_dag
from common.slack_notification import slack_fail_alert

dag = create_dag(
    dag_id="market_size_largest_nft_collections",
    schedule_interval="0 23 * * *",
    description="Build single claimed audiences",
    catchup=False,
    start_date=days_ago(2),
    max_active_runs=1,
    retry_delay=timedelta(minutes=10),
    on_failure_callback=slack_fail_alert,
    tags=["market_size"],
)

with dag:
    start_task_id = "start_task"
    end_task_id = "end_task"
    update_models_task_id = "refresh_market_report_models"

    start_task = EmptyOperator(task_id=start_task_id)
    end_task = EmptyOperator(task_id=end_task_id)

    run = DbtRunOperator(
        dag=dag,
        task_id=update_models_task_id,
        select="report",
    )

    test = DbtTestOperator(
        dag=dag,
        task_id="test",
        select="report",
    )

    start_task >> run >> test >> end_task
