from pycoingecko import CoinGeckoAPI
from requests.exceptions import ConnectionError, HTTPError
from retry import retry

COINS = ['bitcoin', 'litecoin', 'eth', 'tether', 'usd-coin', 'dai']


def get_coins(**kwargs):
    """
    Get USD prices of coins from coingocko the simplest way possible.
    Multiple retries because coingecko could slow down piblic API or 403 it.
    """

    cg = CoinGeckoAPI()

    @retry(tuple([HTTPError, ConnectionError, ValueError]), tries=50, delay=30)
    def _get_coins_current(client):
        return client.get_price(ids=COINS, vs_currencies='usd')

    coins_chunk = _get_coins_current(client=cg)

    return [(row[0], row[1]['usd']) for row in coins_chunk.items()]
