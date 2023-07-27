import clickhouse_connect as cc
from airflow.models import Variable

CONN_ID = 'clickhouse-eth-data'
TABLE_NAME = 'raw_data.binance_prices'


def load_symbols(coin_prices):
    cl = cc.get_client(
        host=Variable.get('clickhouse_host'),
        username=Variable.get('clickhouse_username', default_var='default'),
        password=Variable.get('clickhouse_password'),
    )

    cl.insert(
        table=TABLE_NAME, column_names=['symbol_id', 'symbol_price'], data=coin_prices
    )
