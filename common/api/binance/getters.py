import requests as r
from requests.exceptions import ConnectionError, HTTPError
from retry import retry

TICKER_API = 'https://api.binance.com/api/v3/ticker/price'


def get_all_prices():
    '''
    Get all price tickers with simple price
    '''

    @retry(tuple([HTTPError, ConnectionError, ValueError]), tries=50, delay=30)
    def _get_all_prices(api):
        resp = r.get(api, timeout=10)
        resp.raise_for_status()
        return resp.json()

    symbols = _get_all_prices(TICKER_API)

    return [(s['symbol'], s['price']) for s in symbols]
