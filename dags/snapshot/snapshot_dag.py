from datetime import datetime, timedelta

from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from common.dags_config import create_dag
from common.slack_notification import slack_fail_alert
from common.snapshot.func import export_snapshot_votes, upload_data_to_clickhouse

dag = create_dag(
    dag_id='snapshot_dag',
    schedule_interval='*/5 * * * *',
    on_failure_callback=slack_fail_alert,
    tags=['snapshot', 'votes'],
    start_date=datetime(2023, 4, 16),
    retries=2,
    retry_delay=timedelta(minutes=5),
    depends_on_past=False,
    dbt_project_dir='/opt/dbt_clickhouse',
    dbt_profiles_dir='/opt/airflow/secrets',
    execution_timeout=timedelta(minutes=500),
    dagrun_timeout=timedelta(minutes=500),
    sla=timedelta(minutes=500),
)

start = EmptyOperator(task_id="start", trigger_rule=TriggerRule.ALL_DONE)
finish = EmptyOperator(task_id="finish", trigger_rule=TriggerRule.ALL_DONE)

task_to_pull_from = 'export_snapshot_votes'
export_snapshot_votes_task = PythonOperator(
    dag=dag,
    task_id=f'export_snapshot_votes',
    python_callable=export_snapshot_votes,
    op_kwargs={"blockchain": "ethereum"},
    provide_context=True,
    trigger_rule=TriggerRule.ALL_SUCCESS,
)

upload_data_to_clickhouse_task = PythonOperator(
    dag=dag,
    task_id=f'upload_data_to_clickhouse',
    python_callable=upload_data_to_clickhouse,
    op_kwargs={"task_to_pull_from": task_to_pull_from},
    provide_context=True,
    trigger_rule=TriggerRule.ALL_SUCCESS,
)

start >> export_snapshot_votes_task >> upload_data_to_clickhouse_task >> finish
