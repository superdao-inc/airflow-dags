import logging
import math
import os
import time

import numpy as np
import psycopg2
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow_clickhouse_plugin.hooks.clickhouse_hook import ClickHouseHook
from airflow_provider_kafka.operators.produce_to_topic import ProduceToTopicOperator

from common.connections import CLICKHOUSE_CONNECTION_ID
from common.environment import CURRENT_SCHEMA_SCORING_API
from common.slack_notification import slack_fail_alert

args = {
    "start_date": "2021-05-01",
    "retries": 0,
    "retry_delay": 1,
    "depends_on_past": False,
}

BATCH_SIZE = 1_000_000
DEFAULT_DEANON_ALL_WALLETS_OFFSET_VAR = "deanon_all_wallets_offset"
DEFAULT_DEANON_ALL_WALLETS_OFFSET_MULTIPLIER = 1


@dag(
    dag_id="deanonimization",
    default_args=args,
    catchup=False,
    schedule="0 0 */2 * *",
    tags=["deanon", "twitter"],
    max_active_runs=1,
    on_failure_callback=slack_fail_alert,
)
def twitter_deanonymization(**kwargs):
    """
    This DAG is used to deanonymize twitter audience.
    It can work in two modes:
    1. Deanonymize all wallets from scoring_api.public.mv__wallet_attributes.
    No input table needed.
    2. Deanonymize particular wallets from input table.
    For the second mode, you need to pass input table name in conf:
    {"input": "eth_data.early_adopters_audience_prod"}
    :return:
    """
    # Common
    start_task_id = "start_task"
    finish_task = EmptyOperator(task_id="finish_task")

    # All wallets mode
    all_wallets_start_task_id = "all_wallets_start"
    all_wallets_start_task = EmptyOperator(task_id=all_wallets_start_task_id)
    all_wallets_get_params_task_id = "all_wallets_get_params"
    all_wallets_deanon_task_id = "all_wallets_deanon"
    all_wallets_increment_offset_task_id = "all_wallets_increment_offset"

    # Particular wallets mode
    particular_wallets_start_task_id = "start_particular_wallets"
    particular_wallets_start_task = EmptyOperator(
        task_id=particular_wallets_start_task_id
    )
    particular_wallets_deanon_task_id = "particular_wallets_deanon"

    # Common functions
    def get_prod_scoring_api_conn():
        conn_param = BaseHook.get_connection("pg-prod-scoring-api")
        conn = psycopg2.connect(
            host=conn_param.host,
            port=conn_param.port,
            database=conn_param.schema,
            user=conn_param.login,
            password=conn_param.password,
        )

        return conn

    def get_kafka_params(conf: dict):
        topic = Variable.get("kafka_deanon_produce_topic")
        kafka_topic = conf.get("kafka_topic", topic)
        dev = os.getenv("ENVIRONMENT", "PRODUCTION") == "DEVELOPMENT"
        config_key = "kafka_config_dev" if dev else "kafka_config_prod"
        config = Variable.get(config_key, deserialize_json=True)

        return kafka_topic, config

    def produce_to_kafka(kafka_topic, config, split_array):
        for _, df in enumerate(split_array):
            wallet_addresses = df[:, 0].tolist()
            print(wallet_addresses[:10])

            # define the producer function
            def producer_function():
                for address in wallet_addresses:
                    yield None, address

            ProduceToTopicOperator(
                task_id=f"produce_to_{kafka_topic}",
                topic=kafka_topic,
                producer_function=producer_function,
                kafka_config=config,
                synchronous=False,
            ).execute({})

            time.sleep(1)

    @task.branch(task_id=start_task_id)
    def start(**kwargs):
        conf = kwargs["dag_run"].conf
        input_table = conf["input"] if "input" in conf else None

        if not input_table:
            return all_wallets_start_task_id
        else:
            return particular_wallets_start_task_id

    @task(task_id=all_wallets_get_params_task_id)
    def all_wallets_get_params() -> dict:
        conn = get_prod_scoring_api_conn()

        cursor = conn.cursor()
        cursor.execute(
            f"SELECT count(*) FROM scoring_api.{CURRENT_SCHEMA_SCORING_API}.mv__wallet_attributes"
        )

        count = cursor.fetchone()[0]
        max_batches = math.ceil(count / BATCH_SIZE)

        offset_multipliter = Variable.get(
            DEFAULT_DEANON_ALL_WALLETS_OFFSET_VAR,
            default_var=DEFAULT_DEANON_ALL_WALLETS_OFFSET_MULTIPLIER,
        )
        if not offset_multipliter:
            offset_multipliter = DEFAULT_DEANON_ALL_WALLETS_OFFSET_MULTIPLIER
            Variable.set(DEFAULT_DEANON_ALL_WALLETS_OFFSET_VAR, offset_multipliter)

        return {
            "max_batches": max_batches,
            "offset_multiplier": offset_multipliter,
        }

    @task(task_id=all_wallets_deanon_task_id)
    def all_wallets_deanon(**kwargs):
        ti = kwargs["ti"]
        conf = kwargs["dag_run"].conf

        # get params
        params = ti.xcom_pull(task_ids=all_wallets_get_params_task_id)
        offset_multiplier = int(params["offset_multiplier"])

        conn = get_prod_scoring_api_conn()
        cursor = conn.cursor()
        cursor.execute(
            f"""
                SELECT wallet::text
                FROM scoring_api.{CURRENT_SCHEMA_SCORING_API}.mv__wallet_attributes
                ORDER BY superrank DESC
                LIMIT {BATCH_SIZE} OFFSET {offset_multiplier * BATCH_SIZE}
            """
        )
        rows = cursor.fetchall()

        # split addresses to batches
        batch_size = 10_000
        array_count = math.ceil(len(rows) / batch_size)
        split_array = (
            np.array_split(rows, array_count) if array_count > 0 else [np.array(rows)]
        )

        # produce to kafka
        kafka_topic, config = get_kafka_params(conf)
        produce_to_kafka(kafka_topic, config, split_array)

    @task(task_id=all_wallets_increment_offset_task_id)
    def all_wallets_increment_offset(**kwargs):
        ti = kwargs["ti"]
        params = ti.xcom_pull(task_ids=all_wallets_get_params_task_id)
        max_batches = int(params["max_batches"])
        offset_multiplier = int(params["offset_multiplier"])

        if offset_multiplier >= max_batches:
            Variable.set(
                DEFAULT_DEANON_ALL_WALLETS_OFFSET_VAR,
                DEFAULT_DEANON_ALL_WALLETS_OFFSET_MULTIPLIER,
            )
        else:
            offset_multiplier += 1
            Variable.set(DEFAULT_DEANON_ALL_WALLETS_OFFSET_VAR, offset_multiplier)

    # Particular wallets mode
    @task(task_id=particular_wallets_deanon_task_id)
    def particular_wallets_deanon(**kwargs):
        logging.info("Deanonymization started")

        conf = kwargs["dag_run"].conf
        input_table = conf["input"] if "input" in conf else []

        db_name = input_table.split(".")[0]
        ch_hook = ClickHouseHook(
            clickhouse_conn_id=CLICKHOUSE_CONNECTION_ID, database=db_name
        )
        with ch_hook.get_conn() as conn:
            rows = conn.execute(
                query=f"SELECT wallet FROM {input_table} LIMIT 1000",
                settings={"max_block_size": 1_000_000},
            )

        # split addresses to batches
        batch_size = 10_000
        array_count = math.ceil(len(rows) / batch_size)
        split_array = (
            np.array_split(rows, array_count) if array_count > 0 else [np.array(rows)]
        )

        # produce to kafka
        kafka_topic, config = get_kafka_params(conf)
        produce_to_kafka(kafka_topic, config, split_array)

    start_task = start()

    # All wallets mode
    all_wallets_get_params_task = all_wallets_get_params()
    all_wallets_deanon_task = all_wallets_deanon()
    all_wallets_increment_offset_task = all_wallets_increment_offset()
    (
        start_task
        >> all_wallets_start_task
        >> all_wallets_get_params_task
        >> all_wallets_deanon_task
        >> all_wallets_increment_offset_task
        >> finish_task
    )

    # Particular wallets mode
    particular_wallets_deanon_task = particular_wallets_deanon()
    (
        start_task
        >> particular_wallets_start_task
        >> particular_wallets_deanon_task
        >> finish_task
    )


twitter_deanonymization()
