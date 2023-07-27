import os
from datetime import datetime
from time import sleep

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow_clickhouse_plugin.hooks.clickhouse_hook import ClickHouseHook
from airflow_provider_kafka.operators.produce_to_topic import ProduceToTopicOperator

from common.connections import CLICKHOUSE_CONNECTION_ID
from common.slack_notification import slack_fail_alert

chain_list = {"eth_data": "eth", "polygon_data": "polygon"}


def get_wallets(db_name, chain_name) -> str:
    table = "polygon_attr_nfts_count" if chain_name == "polygon" else "attr_nfts_count"
    db_name = "dbt" if chain_name == "polygon" else db_name

    return f"""
        SELECT DISTINCT address
        FROM {db_name}.{table}
        WHERE nfts_count > 0
    """


start_date = datetime(2023, 4, 11)

default_args = {
    "start_date": "2023-04-11",
    "retries": 0,
    "retry_delay": 2,
    "depends_on_past": False,
}


@dag(
    dag_id="resolve_ens",
    default_args=default_args,
    catchup=False,
    schedule="@monthly",
    start_date=start_date,
    tags=["ens"],
    max_active_runs=1,
    on_failure_callback=slack_fail_alert,
)
def resolve_ens():
    start = EmptyOperator(task_id="start")
    finish = EmptyOperator(task_id="finish")

    topic = Variable.get("kafka_ens_topic", "deanon-ens")
    dev = os.getenv("ENVIRONMENT", "PRODUCTION") == "DEVELOPMENT"
    config_key = "kafka_config_dev" if dev else "kafka_config_prod"

    config = Variable.get(config_key, deserialize_json=True, default_var={})

    eth_task = None
    polygon_task = None
    for chain_db, chain_name in chain_list.items():
        upload_to_kafka_task_id = f"{chain_name}_upload_to_kafka_task_id"

        @task(task_id=upload_to_kafka_task_id)
        def upload_to_kafka(**kwargs):
            print("Running upload_to_kafka")
            conf = kwargs["dag_run"].conf
            kafka_topic = conf.get("kafka_ens_topic", topic)

            ch_hook = ClickHouseHook(
                clickhouse_conn_id=CLICKHOUSE_CONNECTION_ID, database=chain_db
            )
            with ch_hook.get_conn() as conn:
                rows = conn.execute_iter(
                    query=get_wallets(chain_db, chain_name),
                    settings={"max_block_size": 1_000_000},
                    chunk_size=1000,
                )

                # define the producer function
                def producer_function():
                    for row in rows:
                        for elem in row:
                            address = elem[0]
                            yield None, address
                        sleep(1)

                ProduceToTopicOperator(
                    task_id=f"produce_to_{kafka_topic}",
                    topic=kafka_topic,
                    producer_function=producer_function,
                    kafka_config=config,
                    synchronous=False,
                ).execute({})

        if chain_name == "eth":
            eth_task = upload_to_kafka()
        elif chain_name == "polygon":
            polygon_task = upload_to_kafka()

    start >> eth_task >> polygon_task >> finish


resolve_ens()
