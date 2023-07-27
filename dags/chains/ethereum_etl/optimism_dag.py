from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow_dbt.operators.dbt_operator import DbtRunOperator, DbtTestOperator

from common.chains.ethereum.ethereum_etl.extractions import (
    export_data,
    replace_data_to_raw,
    upload_data_to_clickhouse,
)
from common.chains.ethereum.ethereum_etl.functions import ethereum_etl_prepare_data
from common.dags_config import create_dag

# from airflow.models import Variable
from common.slack_notification import slack_fail_alert

# ETHEREUM_BATCH_SIZE = int(Variable.get("ethereum_etl_batch_size", 1000))
# ETHEREUM_PROVIDER_URI = Variable.get("ethereum_provider_uri", "http://eth.superdao.dev")
#
# ETHEREUM_ETL_DATABASE = "raw_data_ethereum"
# ETHEREUM_CHAIN_NAME = "ethereum"
# ETHEREUM_ALCHEMY_PROVIDER = "https://eth-mainnet.g.alchemy.com/v2/g291wLkt4utHpE9pWjA0XkStzwZFnvoP"
resource_config = {
    "KubernetesExecutor": {"request_memory": "5000Mi", "request_cpu": "6000m"}
}


dag = create_dag(
    dag_id='optimism_etl_dag',
    schedule_interval='*/2 * * * *',
    on_failure_callback=slack_fail_alert,
    tags=['etl', 'optimism'],
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

start = EmptyOperator(task_id="start", trigger_rule=TriggerRule.ALL_DONE)
finish = EmptyOperator(task_id="finish", trigger_rule=TriggerRule.ALL_DONE)

test_source = DbtTestOperator(
    dag=dag,
    task_id='dbt_test_all_buf_optimism_source',
    retries=1,
    select='source:buf_raw_data_optimism',
    trigger_rule=TriggerRule.ALL_SUCCESS,
)

ethereum_etl_prepare_data = PythonOperator(
    dag=dag,
    task_id=f'ethereum_etl_prepare_data',
    python_callable=ethereum_etl_prepare_data,
    op_kwargs={"blockchain": "optimism", "batch_size": 50000},
    provide_context=True,
)

start >> ethereum_etl_prepare_data

for entity in ['blocks', 'transactions']:  # , 'traces']:
    task_to_pull_from = f'ethereum_etl_export_data_{entity}'
    entity_export_data = PythonOperator(
        dag=dag,
        task_id=task_to_pull_from,
        python_callable=export_data,
        op_kwargs={
            "table": entity,
            "task_to_pull_from": "ethereum_etl_prepare_data",
            "blockchain": "optimism",
        },
        provide_context=True,
        executor_config=resource_config,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    entity_upload_data_to_clickhouse = PythonOperator(
        dag=dag,
        task_id=f'ethereum_etl_upload_data_to_clickhouse_{entity}',
        python_callable=upload_data_to_clickhouse,
        op_kwargs={
            "table": entity,
            "task_to_pull_from": task_to_pull_from,
            "blockchain": "optimism",
        },
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    entity_dbt_upload_into_raw_data = PythonOperator(
        dag=dag,
        task_id=f'replace_data_{entity}',
        python_callable=replace_data_to_raw,
        op_kwargs={"table": entity, "blockchain": "optimism"},
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # if entity == 'traces':
    #     task_to_pull_from = f'ethereum_etl_export_data_contracts'
    #     contracts_export_data = PythonOperator(dag=dag,
    #                                                 task_id=task_to_pull_from,
    #                                                 python_callable=export_data,
    #                                                 op_kwargs={"table": "contracts", "task_to_pull_from": "ethereum_etl_prepare_data", "blockchain": "optimism"},
    #                                                 provide_context=True,
    #                                                 executor_config=resource_config,
    #                                                 trigger_rule=TriggerRule.ALL_SUCCESS)

    #     contracts_upload_data_to_clickhouse = PythonOperator(dag=dag,
    #                                                     task_id=f'ethereum_etl_upload_data_to_clickhouse_contracts',
    #                                                     python_callable=upload_data_to_clickhouse,
    #                                                     op_kwargs={"table": "contracts", "task_to_pull_from": task_to_pull_from, "blockchain": "optimism"},
    #                                                     provide_context=True,
    #                                                     trigger_rule=TriggerRule.ALL_SUCCESS)

    #     contracts_dbt_upload_into_raw_data = PythonOperator(dag=dag,
    #                                                            task_id=f'replace_data_contracts',
    #                                                            python_callable=replace_data_to_raw,
    #                                                            op_kwargs={"table": "contracts", "blockchain": "optimism"},
    #                                                            provide_context=True,
    #                                                            trigger_rule=TriggerRule.ALL_SUCCESS)

    #     entity_upload_data_to_clickhouse >> contracts_export_data >> contracts_upload_data_to_clickhouse >> test_source >> contracts_dbt_upload_into_raw_data >> finish
    (
        ethereum_etl_prepare_data
        >> entity_export_data
        >> entity_upload_data_to_clickhouse
        >> test_source
        >> entity_dbt_upload_into_raw_data
        >> finish
    )

    if entity == 'transactions':
        for dep_entity in ['receipts', 'logs']:
            task_to_pull_from = f'ethereum_etl_export_data_{dep_entity}'
            dep_export_data = PythonOperator(
                dag=dag,
                task_id=task_to_pull_from,
                python_callable=export_data,
                op_kwargs={
                    "table": dep_entity,
                    "task_to_pull_from": "ethereum_etl_prepare_data",
                    "blockchain": "optimism",
                },
                provide_context=True,
                executor_config=resource_config,
                trigger_rule=TriggerRule.ALL_SUCCESS,
            )

            dep_upload_data_to_clickhouse = PythonOperator(
                dag=dag,
                task_id=f'ethereum_etl_upload_data_to_clickhouse_{dep_entity}',
                python_callable=upload_data_to_clickhouse,
                op_kwargs={
                    "table": dep_entity,
                    "task_to_pull_from": task_to_pull_from,
                    "blockchain": "optimism",
                },
                provide_context=True,
                trigger_rule=TriggerRule.ALL_SUCCESS,
            )

            dep_dbt_upload_into_raw_data = PythonOperator(
                dag=dag,
                task_id=f'replace_data_{dep_entity}',
                python_callable=replace_data_to_raw,
                op_kwargs={"table": dep_entity, "blockchain": "optimism"},
                provide_context=True,
                trigger_rule=TriggerRule.ALL_SUCCESS,
            )

            if dep_entity == 'logs':
                task_to_pull_from = f'ethereum_etl_export_data_token_transfers'
                token_transfers_export_data = PythonOperator(
                    dag=dag,
                    task_id=task_to_pull_from,
                    python_callable=export_data,
                    op_kwargs={
                        "table": "token_transfers",
                        "task_to_pull_from": "ethereum_etl_prepare_data",
                        "blockchain": "optimism",
                    },
                    provide_context=True,
                    executor_config=resource_config,
                    trigger_rule=TriggerRule.ALL_SUCCESS,
                )

                token_transfers_upload_data_to_clickhouse = PythonOperator(
                    dag=dag,
                    task_id=f'ethereum_etl_upload_data_to_clickhouse_token_transfers',
                    python_callable=upload_data_to_clickhouse,
                    op_kwargs={
                        "table": "token_transfers",
                        "task_to_pull_from": task_to_pull_from,
                        "blockchain": "optimism",
                    },
                    provide_context=True,
                    trigger_rule=TriggerRule.ALL_SUCCESS,
                )

                token_transfers_dbt_upload_into_raw_data = PythonOperator(
                    dag=dag,
                    task_id=f'replace_data_token_transfers',
                    python_callable=replace_data_to_raw,
                    op_kwargs={"table": "token_transfers", "blockchain": "optimism"},
                    provide_context=True,
                    trigger_rule=TriggerRule.ALL_SUCCESS,
                )

                (
                    dep_upload_data_to_clickhouse
                    >> token_transfers_export_data
                    >> token_transfers_upload_data_to_clickhouse
                    >> test_source
                    >> token_transfers_dbt_upload_into_raw_data
                    >> finish
                )
                (
                    ethereum_etl_prepare_data
                    >> entity_export_data
                    >> entity_upload_data_to_clickhouse
                    >> dep_export_data
                    >> dep_upload_data_to_clickhouse
                    >> test_source
                    >> dep_dbt_upload_into_raw_data
                )
                test_source >> entity_dbt_upload_into_raw_data >> finish
            else:
                (
                    ethereum_etl_prepare_data
                    >> entity_export_data
                    >> entity_upload_data_to_clickhouse
                    >> dep_export_data
                    >> dep_upload_data_to_clickhouse
                    >> test_source
                    >> dep_dbt_upload_into_raw_data
                    >> finish
                )
                test_source >> entity_dbt_upload_into_raw_data >> finish

    else:
        (
            ethereum_etl_prepare_data
            >> entity_export_data
            >> entity_upload_data_to_clickhouse
            >> test_source
            >> entity_dbt_upload_into_raw_data
            >> finish
        )
