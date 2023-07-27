from airflow_clickhouse_plugin.hooks.clickhouse_hook import ClickHouseHook

from common.connections import CLICKHOUSE_CONNECTION_ID

CONN_ID = 'clickhouse-eth-data'
TABLE_NAME = 'raw_data.coin_prices'


def load_coingecko(coin_prices):
    ch_hook = ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_CONNECTION_ID)

    query = f'INSERT INTO {TABLE_NAME} (coin_id, coin_price) VALUES '

    ch_hook.run(sql=query, parameters=coin_prices)
