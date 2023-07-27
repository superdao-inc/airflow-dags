import logging
import re
from datetime import datetime, timedelta

from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from airflow_clickhouse_plugin.hooks.clickhouse_hook import ClickHouseHook
from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator
from airflow_dbt.operators.dbt_operator import DbtRunOperator
from cron_validator import CronValidator

from common.connections import CLICKHOUSE_CONNECTION_ID
from common.dag_utils import clh2pg_tasks_generator
from common.dags_config import create_dag
from common.environment import CURRENT_SCHEMA_DBT, CURRENT_SCHEMA_SCORING_API
from common.slack_notification import slack_fail_alert

DAG_ID = 'build_single_claimed_audiences'
PARTITION_SIZE = 150_000
PG_TARGET_TABLE = 'claimed'

default_args = {
    "start_date": datetime(2023, 4, 24),
}

dag = create_dag(
    dag_id=DAG_ID,
    schedule_interval=None,
    description="Build single claimed audiences",
    catchup=False,
    start_date=days_ago(2),
    max_active_runs=1,
    retry_delay=timedelta(minutes=10),
    on_failure_callback=slack_fail_alert,
    dbt_profiles_dir=None,
    dbt_project_dir=None,
)

with dag:
    start_task = EmptyOperator(task_id="start")
    finish_task = EmptyOperator(task_id="finish")

    def get_insert_params_sql(
        collection_contract, ch_database, claimed_schedule, is_fungible
    ):
        schedule = f"'{claimed_schedule}'" if claimed_schedule is not None else "NULL"
        insert_sql = f"""
            INSERT INTO {ch_database}.input_claimed_audiences
            VALUES ('{collection_contract}', '{ch_database}', '{is_fungible}', now(), {schedule})
        """
        optimize_sql = f"OPTIMIZE TABLE {ch_database}.input_claimed_audiences FINAL"

        return [insert_sql, optimize_sql]

    def get_create_partition_sql(collection_contract):
        create_sql = f"""
            CREATE TABLE claimed_{collection_contract} PARTITION OF {PG_TARGET_TABLE} FOR VALUES IN ('{collection_contract}');
        """
        pass_owner_sql = (
            f"ALTER TABLE claimed_{collection_contract} OWNER TO 'scoring_api';"
        )

        return [create_sql, pass_owner_sql]

    input_validation_task_id = "input_validation"
    save_input_params_task_id = "save_input_params"

    @task(task_id=input_validation_task_id)
    def input_validation(**kwargs):
        conf = kwargs["dag_run"].conf

        logging.info("Validating input", conf)

        collection_contract = conf.get("collection_contract", None)
        claimed_schedule = conf.get("schedule", None)
        is_fungible = conf.get("is_fungible", False)
        ch_database = conf.get("ch_database", None)

        address_regex = re.compile(r"^0x[a-fA-F0-9]{40}$")

        if collection_contract is None or not address_regex.match(collection_contract):
            raise AirflowException("Collection contract is not valid")

        ch_hook = ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_CONNECTION_ID)

        # Check if we have this collection contract
        sql = f"SELECT * from {ch_database}.contracts_by_address WHERE address = '{collection_contract}'"
        result = ch_hook.get_first(sql)
        if result is None:
            raise AirflowException("Collection contract does not exist")

        # Check if we already have this collection contract in input_claimed_audiences
        sql = f"SELECT * from {ch_database}.input_claimed_audiences WHERE collection_contract = '{collection_contract}'"
        result = ch_hook.get_first(sql)
        if result is not None:
            raise AirflowException(
                "Collection contract is already in input_claimed_audiences"
            )

        if (
            claimed_schedule is not None
            and CronValidator.parse(claimed_schedule) is None
        ):
            raise AirflowException("Schedule should be a valid crontab expression")

        if not isinstance(is_fungible, bool):
            raise AirflowException("is_fungible should be a boolean")

        if ch_database is None or ch_database not in [
            "eth_data",
            "polygon_data",
            "ethereum_raw_data",
            "polygon_raw_data",
        ]:
            raise AirflowException("ch_database is not valid")

        return {
            "collection_contract": collection_contract.lower(),
            "ch_database": ch_database,
            "claimed_schedule": claimed_schedule,
            "is_fungible": is_fungible,
        }

    @task(task_id=save_input_params_task_id)
    def save_input_params(**kwargs):
        logging.info("Saving input params")

        ti = kwargs["ti"]
        input_data = ti.xcom_pull(task_ids=input_validation_task_id)
        collection_contract = input_data["collection_contract"]
        ch_database = input_data["ch_database"]
        claimed_schedule = input_data["claimed_schedule"]
        is_fungible = input_data["is_fungible"]

        ch_operator = ClickHouseOperator(
            task_id="ch_operator",
            clickhouse_conn_id=CLICKHOUSE_CONNECTION_ID,
            database=ch_database,
            sql=get_insert_params_sql(
                collection_contract, ch_database, claimed_schedule, is_fungible
            ),
        )
        ch_operator.execute(context=kwargs)

    build_audience_operator = DbtRunOperator(
        vars={
            "collection_contract": "{{ ti.xcom_pull(task_ids='input_validation')['collection_contract'] }}",
            "ch_database": "{{ ti.xcom_pull(task_ids='input_validation')['ch_database'] }}",
            "is_fungible": "{{ ti.xcom_pull(task_ids='input_validation')['is_fungible'] }}",
        },
        task_id=f"dbt_run_single_claimed_audience",
        models="single_claimed_audience",
        dag=dag,
    )

    @task(task_id="check_count")
    def check_count(**kwargs):
        clickhouse_operator = ClickHouseOperator(
            clickhouse_conn_id=CLICKHOUSE_CONNECTION_ID,
            task_id="check_count",
            sql=f'SELECT count(*) FROM {CURRENT_SCHEMA_DBT}.single_claimed_audience',
        )

        return clickhouse_operator.execute(context=kwargs)[0][0]

    @task(task_id="create_partition")
    def create_partition(**kwargs):
        ti = kwargs["ti"]

        input_data, count = ti.xcom_pull(task_ids=['input_validation', 'check_count'])
        collection_contract = input_data["collection_contract"]

        create_partition = EmptyOperator(task_id="create_partition")
        if count > PARTITION_SIZE:
            create_partition = PostgresOperator(
                postgres_conn_id="postgres_clickhouse",
                task_id="create_partition",
                sql=get_create_partition_sql(collection_contract),
            )

        create_partition.execute(context=kwargs)

    partition_task = create_partition()

    query_sql = f"""
        SELECT DISTINCT ON (wallet)
            wallet,
            if(chain = 'eth', 'ETHEREUM', 'POLYGON') as blockchain,
            token_address as claimed_contract,
            concat('\\\\x', upper(substring(wallet, 3))) as wallet_b
        FROM {CURRENT_SCHEMA_DBT}.single_claimed_audience
    """
    clh2pg_tasks_generator(
        start_task=partition_task,
        finish_task=finish_task,
        table_data={
            'ch_db': CURRENT_SCHEMA_DBT,
            'pg_schema': CURRENT_SCHEMA_SCORING_API,
            'ch_table': 'single_claimed_audience',
            'pg_table': PG_TARGET_TABLE,
        },
        query=query_sql,
        truncate=False,
        dag_id=DAG_ID,
    )

    (
        start_task
        >> input_validation()
        >> save_input_params()
        >> build_audience_operator
        >> check_count()
        >> partition_task
    )
