from datetime import datetime

from airflow import DAG
from airflow.decorators import dag
from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator

from common.connections import CLICKHOUSE_CONNECTION_ID
from common.environment import CURRENT_SCHEMA_DBT
from common.slack_notification import slack_fail_alert

args = {
    "start_date": datetime(2023, 2, 15),
    "on_failure_callback": slack_fail_alert,
    "tags": ["attribute", "wallet_score"],
    "catchup": False,
    "depends_on_past": False,
}


def get_update_wallet_score_sql(
    db_name: str,
    score_id: str,
    score_lower_bound: int,
    blacklisted_wallets_table: str,
) -> str:
    return f"""
        INSERT INTO {db_name}.wallet_score

        WITH
            toDecimal32(5.5, 1) as BASE_HAS_NFT_TOKEN_WEIGHT,
            toDecimal32(1.5, 1) as BASE_HAD_NFT_TOKEN_WEIGHT,
            toDecimal32(3, 1) as BASE_HAS_FUNGIBLE_TOKEN_WEIGHT,
            toDecimal32(0.5, 1) as BASE_HAD_FUNGIBLE_TOKEN_WEIGHT,

            all_contracts_subquery as (
                SELECT DISTINCT address
                FROM {db_name}.contracts_by_address
            ),

            fungible_contracts_subquery as (
                SELECT DISTINCT address
                FROM {db_name}.macro_audiences_signals
                WHERE is_fungible=true AND score_id='{score_id}'
            ),
            
            -- ERC721
            signal_erc721_contracts_holders_subquery as (
                SELECT DISTINCT
                    last_value(transfers.to_address)
                    OVER (
                        PARTITION BY transfers.token_address, transfers.value
                        ORDER BY transfers.block_number, transfers.log_index
                        RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                    ) AS wallet_address,
                    transfers.token_address as token_address,
                    coalesce(signals.weight, BASE_HAS_NFT_TOKEN_WEIGHT) as has_token_weight,
                    toDecimal32(0, 1) as had_token_weight
                FROM {db_name}.token_transfers_by_token as transfers
                INNER JOIN {db_name}.macro_audiences_signals as signals
                    ON signals.address = transfers.token_address
                WHERE
                    signals.score_id = '{score_id}'
                    AND is_fungible = false
                    AND transfers.to_address NOT IN all_contracts_subquery
            ),
            signal_erc721_contracts_transferees_subquery as (
                SELECT DISTINCT
                    transfers.to_address as wallet_address,
                    transfers.token_address as token_address,
                    toDecimal32(0, 1) as has_token_weight,
                    BASE_HAD_NFT_TOKEN_WEIGHT as had_token_weight
                FROM {db_name}.token_transfers_by_token as transfers
                INNER JOIN {db_name}.macro_audiences_signals as signals
                    ON signals.address = transfers.token_address
                WHERE
                    signals.score_id = '{score_id}'
                    AND is_fungible = false
                    AND transfers.to_address NOT IN all_contracts_subquery
            ),

            -- ERC20
            signal_erc20_contracts_holders_subquery as (
                SELECT DISTINCT
                    wallet as wallet_address,
                    token_address,
                    BASE_HAS_FUNGIBLE_TOKEN_WEIGHT as has_token_weight,
                    toDecimal32(0, 1) as had_token_weight
                FROM (
                        SELECT token_address,
                            to_address           AS wallet,
                            toInt256(sum(value)) AS income
                        FROM {db_name}.token_transfers_by_token
                        WHERE token_address IN fungible_contracts_subquery
                        GROUP BY token_address, to_address
                ) incomes
                FULL JOIN (
                        SELECT token_address,
                            from_address         AS wallet,
                            toInt256(sum(value)) AS outcome
                        FROM {db_name}.token_transfers_by_token
                        WHERE token_address IN fungible_contracts_subquery
                        GROUP BY token_address, from_address
                ) outcomes ON incomes.wallet = outcomes.wallet
                WHERE incomes.income - outcomes.outcome > 0
                    AND wallet NOT IN all_contracts_subquery
                    AND wallet NOT IN {db_name}.blacklisted_wallets_common
            ),
            signal_erc20_contracts_transferees_subquery as (
                SELECT DISTINCT
                    transfers.to_address as wallet_address,
                    transfers.token_address as token_address,
                    toDecimal32(0, 1) as has_token_weight,
                    BASE_HAD_FUNGIBLE_TOKEN_WEIGHT as had_token_weight
                FROM {db_name}.token_transfers_by_token as transfers
                WHERE transfers.token_address IN fungible_contracts_subquery
                AND transfers.to_address NOT IN all_contracts_subquery
            )

        SELECT
            '{score_id}' as score_id,
            wallet_address,
            sum(has_token_weight) + sum(had_token_weight) as score,
            now() as updated
        FROM (
            SELECT * FROM signal_erc721_contracts_holders_subquery
            UNION ALL
            SELECT * FROM signal_erc721_contracts_transferees_subquery
            UNION ALL
            SELECT * FROM signal_erc20_contracts_holders_subquery
            UNION ALL
            SELECT * FROM signal_erc20_contracts_transferees_subquery
        ) as wallet_tokens
        WHERE wallet_address NOT IN {db_name}.blacklisted_wallets_common
            {f'AND wallet_address NOT IN {db_name}.{blacklisted_wallets_table}' if blacklisted_wallets_table else ''}

        GROUP BY wallet_address
        HAVING score >= {score_lower_bound}
        ORDER BY score DESC
        """


eth_macro_audiences = [
    {
        "score_id": "developers_score",
        "blacklisted_wallets_table": "blacklisted_wallets_developers",
        "score_lower_bound": 8,
        "cron": "0 2 * * *",
    },
    {
        "score_id": "crypto_natives_score",
        "blacklisted_wallets_table": "blacklisted_wallets_early_adopters",
        "score_lower_bound": 8,
        "cron": "0 2 * * *",
    },
    {
        "score_id": "defi_score",
        "blacklisted_wallets_table": "",
        "score_lower_bound": 16,
        "cron": "10 2 * * *",
    },
    {
        "score_id": "gaming_score",
        "blacklisted_wallets_table": "",
        "score_lower_bound": 10,
        "cron": "10 2 * * *",
    },
    {
        "score_id": "art_score",
        "blacklisted_wallets_table": "",
        "score_lower_bound": 8,
        "cron": "20 2 * * *",
    },
    {
        "score_id": "fashion_score",
        "blacklisted_wallets_table": "",
        "score_lower_bound": 8,
        "cron": "20 2 * * *",
    },
    {
        "score_id": "luxury_score",
        "blacklisted_wallets_table": "",
        "score_lower_bound": 8,
        "cron": "30 2 * * *",
    },
    {
        "score_id": "music_score",
        "blacklisted_wallets_table": "",
        "score_lower_bound": 8,
        "cron": "30 2 * * *",
    },
    {
        "score_id": "professional_score",
        "blacklisted_wallets_table": "",
        "score_lower_bound": 8,
        "cron": "40 2 * * *",
    },
    {
        "score_id": "investor_score",
        "blacklisted_wallets_table": "",
        "score_lower_bound": 16,
        "cron": "40 2 * * *",
    },
    {
        "score_id": "donor_score",
        "blacklisted_wallets_table": "",
        "score_lower_bound": 4,
        "cron": "50 2 * * *",
    },
]

polygon_macro_audiences = [
    {
        "score_id": "developers_score",
        "blacklisted_wallets_table": "",
        "score_lower_bound": 20,
        "cron": "0 2 * * *",
    },
    {
        "score_id": "crypto_natives_score",
        "blacklisted_wallets_table": "",
        "score_lower_bound": 8,
        "cron": "0 2 * * *",
    },
    {
        "score_id": "defi_score",
        "blacklisted_wallets_table": "",
        "score_lower_bound": 15,
        "cron": "10 2 * * *",
    },
    {
        "score_id": "gaming_score",
        "blacklisted_wallets_table": "",
        "score_lower_bound": 10,
        "cron": "40 2 * * *",
    },
    {
        "score_id": "art_score",
        "blacklisted_wallets_table": "",
        "score_lower_bound": 8,
        "cron": "20 2 * * *",
    },
    {
        "score_id": "fashion_score",
        "blacklisted_wallets_table": "",
        "score_lower_bound": 8,
        "cron": "20 2 * * *",
    },
    {
        "score_id": "luxury_score",
        "blacklisted_wallets_table": "",
        "score_lower_bound": 8,
        "cron": "30 2 * * *",
    },
    {
        "score_id": "music_score",
        "blacklisted_wallets_table": "",
        "score_lower_bound": 8,
        "cron": "30 2 * * *",
    },
    {
        "score_id": "donor_score",
        "blacklisted_wallets_table": "",
        "score_lower_bound": 4,
        "cron": "40 2 * * *",
    },
    {
        "score_id": "professional_score",
        "blacklisted_wallets_table": "",
        "score_lower_bound": 8,
        "cron": "40 2 * * *",
    },
]


def build_wallet_score_dag(
    db_name: str,
    score_id: str,
    score_lower_bound: int,
    blacklisted_wallets_table: str,
    schedule: str = "@daily",
) -> DAG:
    @dag(
        dag_id=f"{db_name}_{score_id}",
        default_args=args,
        schedule=schedule,
        max_active_runs=1,
        catchup=False,
        tags=["wallet_score", db_name, score_id],
    )
    def wallet_score():
        sql = get_update_wallet_score_sql(
            db_name=db_name,
            score_id=score_id,
            score_lower_bound=score_lower_bound,
            blacklisted_wallets_table=blacklisted_wallets_table,
        )

        delete_score_task = ClickHouseOperator(
            task_id=f"truncate_{score_id}",
            clickhouse_conn_id=CLICKHOUSE_CONNECTION_ID,
            sql=f"ALTER TABLE {db_name}.wallet_score DELETE WHERE score_id='{score_id}' SETTINGS mutations_sync = 2",
        )

        update_score_task = ClickHouseOperator(
            task_id=f"update_{score_id}",
            clickhouse_conn_id=CLICKHOUSE_CONNECTION_ID,
            sql=sql,
        )

        optimize = ClickHouseOperator(
            task_id=f"optimize_{db_name}_wallet_score",
            clickhouse_conn_id=CLICKHOUSE_CONNECTION_ID,
            sql=f"OPTIMIZE TABLE {db_name}.wallet_score FINAL",
        )

        delete_score_task >> update_score_task >> optimize

    return wallet_score()


for eth_audience in eth_macro_audiences:
    eth_score_id = eth_audience["score_id"]
    eth_blacklisted_wallets_table = eth_audience["blacklisted_wallets_table"]
    eth_score_lower_bound = eth_audience["score_lower_bound"]
    eth_cron_expr = eth_audience["cron"]

    eth_wallet_score_dag: DAG = build_wallet_score_dag(
        db_name="eth_data",
        score_id=eth_score_id,
        blacklisted_wallets_table=eth_blacklisted_wallets_table,
        score_lower_bound=eth_score_lower_bound,
        schedule=eth_cron_expr,
    )

for polygon_audience in polygon_macro_audiences:
    polygon_score_id = polygon_audience["score_id"]
    polygon_blacklisted_wallets_table = polygon_audience["blacklisted_wallets_table"]
    polygon_score_lower_bound = polygon_audience["score_lower_bound"]
    polygon_cron_expr = polygon_audience["cron"]

    polygon_wallet_score_dag: DAG = build_wallet_score_dag(
        db_name="polygon_data",
        score_id=polygon_score_id,
        score_lower_bound=polygon_score_lower_bound,
        blacklisted_wallets_table=polygon_blacklisted_wallets_table,
        schedule=polygon_cron_expr,
    )


def build_culture_score(ch_db, schedule_interval):
    dag_id = f"{ch_db}_culture_score"

    @dag(
        dag_id=dag_id,
        default_args=args,
        schedule=schedule_interval,
        max_active_runs=1,
        catchup=False,
        tags=["wallet_score", ch_db, "culture_score"],
    )
    def culture_score_dag():
        culture_score_id = "culture_score"

        CULTURE_SCORE_SQL = f"""--sql
            INSERT INTO {ch_db}.wallet_score
            SELECT '{culture_score_id}' as scord_id, wallet, sum(score) AS score, now() AS updated
            FROM {ch_db}.wallet_score
            WHERE score_id IN ('art_score', 'fashion_score', 'luxury_score', 'music_score')
            AND wallet NOT IN {ch_db}.blacklisted_wallets_common
            GROUP BY wallet;
        """

        delete_score_task = ClickHouseOperator(
            task_id=f"truncate_{culture_score_id}",
            clickhouse_conn_id=CLICKHOUSE_CONNECTION_ID,
            sql=f"ALTER TABLE {ch_db}.wallet_score DELETE WHERE score_id='{culture_score_id}' SETTINGS  mutations_sync = 2",
        )

        update_score_task = ClickHouseOperator(
            task_id=f"update_{culture_score_id}",
            clickhouse_conn_id=CLICKHOUSE_CONNECTION_ID,
            sql=CULTURE_SCORE_SQL,
        )

        optimize = ClickHouseOperator(
            task_id=f"optimize_eth_wallet_score",
            clickhouse_conn_id=CLICKHOUSE_CONNECTION_ID,
            sql=f"OPTIMIZE TABLE {ch_db}.wallet_score FINAL",
        )

        delete_score_task >> update_score_task >> optimize

    return culture_score_dag()


build_culture_score("eth_data", "0 3 * * *")
build_culture_score("polygon_data", "10 3 * * *")


def build_label_audiences(db_name: str, table_name: str, schedule_interval: str):
    dag_id = f"{table_name}_label_audiences"

    LABEL_AUDIENCE_SQL = f"""--sql
        INSERT INTO {db_name}.wallet_score
        SELECT DISTINCT 'label:' || label AS scord_id, address AS wallet, 0 AS score, now() AS updated
        FROM {CURRENT_SCHEMA_DBT}.{table_name}
        ARRAY JOIN labels as label
        WHERE label not in('zombie', 'passive');
    """

    @dag(
        dag_id=dag_id,
        default_args=args,
        schedule=schedule_interval,
        max_active_runs=1,
        catchup=False,
        tags=["wallet_score", table_name, "label_audiences"],
    )
    def label_audiences_dag():
        delete_task_id = f"delete_{table_name}"
        insert_task_id = f"insert_{table_name}"
        optimize_task_id = f"optimize_{table_name}"

        delete_label_audiences_task = ClickHouseOperator(
            task_id=delete_task_id,
            clickhouse_conn_id=CLICKHOUSE_CONNECTION_ID,
            sql=f"ALTER TABLE {db_name}.wallet_score DELETE WHERE score_id ILIKE 'label:%' SETTINGS mutations_sync = 2",
        )

        insert_label_audiences_task = ClickHouseOperator(
            task_id=insert_task_id,
            clickhouse_conn_id=CLICKHOUSE_CONNECTION_ID,
            sql=LABEL_AUDIENCE_SQL,
        )

        optimize = ClickHouseOperator(
            task_id=optimize_task_id,
            clickhouse_conn_id=CLICKHOUSE_CONNECTION_ID,
            sql=f"OPTIMIZE TABLE {db_name}.wallet_score FINAL",
        )

        delete_label_audiences_task >> insert_label_audiences_task >> optimize

    return label_audiences_dag()


build_label_audiences("polygon_data", "polygon_attr_labels", "20 3 * * *")
build_label_audiences("eth_data", "eth_attr_labels", "30 3 * * *")
