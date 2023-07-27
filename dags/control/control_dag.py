from datetime import datetime, timedelta

from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.trigger_rule import TriggerRule

from common.control.func import RunDagAPIOperator
from common.dags_config import create_dag
from common.slack_notification import slack_fail_alert

dag = create_dag(
    dag_id='control_dag',
    schedule_interval=None,
    on_failure_callback=slack_fail_alert,
    tags=['control_dag', 'control'],
    start_date=datetime(2023, 4, 16),
    retries=2,
    retry_delay=timedelta(minutes=5),
    depends_on_past=False,
    dbt_project_dir='/opt/dbt_clickhouse',
    dbt_profiles_dir='/opt/airflow/secrets',
    execution_timeout=timedelta(minutes=2000),
    dagrun_timeout=timedelta(minutes=2000),
    sla=timedelta(minutes=2000),
)

start = EmptyOperator(task_id="start", trigger_rule=TriggerRule.ALL_DONE)
finish = EmptyOperator(task_id="finish", trigger_rule=TriggerRule.ALL_DONE)

update_raw_data_task = RunDagAPIOperator(
    dag=dag, task_id='update_raw_data', dag_run='upload_raw_data_into_pipeline'
)
update_raw_data_sensor = ExternalTaskSensor(
    task_id=f'sensor_update_raw_data',
    external_dag_id='upload_raw_data_into_pipeline',
    external_task_id=None,  # Wait for any task in the DAG to complete
    mode='reschedule',  # Keep checking until the task completes
    timeout=7200,  # Timeout after 1 hour (adjust as needed)
    poke_interval=60,  # Check every 60 seconds (adjust as needed)
    dag=dag,
)

start >> update_raw_data_task >> update_raw_data_sensor


update_dbt_test_ethereum_etl_task = RunDagAPIOperator(
    dag=dag, task_id='update_dbt_test_ethereum_etl', dag_run='dbt_test_ethereum_etl'
)
update_dbt_test_ethereum_etl_sensor = ExternalTaskSensor(
    task_id=f'sensor_update_dbt_test_ethereum_etl',
    external_dag_id='dbt_test_ethereum_etl',
    external_task_id=None,  # Wait for any task in the DAG to complete
    mode='reschedule',  # Keep checking until the task completes
    timeout=360000,  # Timeout after 1 hour (adjust as needed)
    poke_interval=60,  # Check every 60 seconds (adjust as needed)
    dag=dag,
)

(
    update_raw_data_sensor
    >> update_dbt_test_ethereum_etl_task
    >> update_dbt_test_ethereum_etl_sensor
)

update_monodag_task = RunDagAPIOperator(
    dag=dag, task_id=f'update_monodag', dag_run='v3_monodag'
)
monodag_sensor = ExternalTaskSensor(
    task_id=f'sensor_update_monodag',
    external_dag_id='v3_monodag',
    external_task_id=None,  # Wait for any task in the DAG to complete
    mode='reschedule',  # Keep checking until the task completes
    timeout=360000,  # Timeout after 1 hour (adjust as needed)
    poke_interval=60,  # Check every 60 seconds (adjust as needed)
    dag=dag,
)

update_dbt_test_ethereum_etl_sensor >> update_monodag_task >> monodag_sensor

update_top_collections_revisited_task = RunDagAPIOperator(
    dag=dag,
    task_id=f'update_top_collections_revisited',
    dag_run='top_collections_revisited',
)
dag_top_collections_revisited_sensor = ExternalTaskSensor(
    task_id=f'sensor_update_top_collections_revisited',
    external_dag_id='top_collections_revisited',
    external_task_id=None,  # Wait for any task in the DAG to complete
    mode='reschedule',  # Keep checking until the task completes
    timeout=360000,  # Timeout after 1 hour (adjust as needed)
    poke_interval=600,  # Check every 60 seconds (adjust as needed)
    dag=dag,
)

(
    monodag_sensor
    >> update_top_collections_revisited_task
    >> dag_top_collections_revisited_sensor
)

replication_dags = EmptyOperator(
    task_id="replication_dags", trigger_rule=TriggerRule.ALL_SUCCESS
)

for dag_name in ['attributes_replication_dbt']:
    update_dag_task = RunDagAPIOperator(
        dag=dag, task_id=f'update_{dag_name}', dag_run=dag_name
    )
    dag_sensor = ExternalTaskSensor(
        task_id=f'sensor_update_{dag_name}',
        external_dag_id=dag_name,
        external_task_id=None,  # Wait for any task in the DAG to complete
        mode='reschedule',  # Keep checking until the task completes
        timeout=360000,  # Timeout after 1 hour (adjust as needed)
        poke_interval=600,  # Check every 60 seconds (adjust as needed)
        dag=dag,
    )
    monodag_sensor >> update_dag_task >> dag_sensor >> replication_dags >> finish
