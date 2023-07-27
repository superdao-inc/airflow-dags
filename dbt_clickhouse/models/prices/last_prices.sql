{{
    config(
        engine = 'MergeTree()',
        order_by = 'symbol_id',
        materialized = 'table'
    )
}}
select
    bp.symbol_id
    , max(coin_id) as coin_id
    , argMax(bp.symbol_price, bp.load_timest) as price_usdt
    , max(bp.load_timest) as load_timest
from {{source('prices', 'binance_prices')}} bp
    join {{ref('binance_symbol_map')}} sm on sm.binance_symbol = bp.symbol_id
group by bp.symbol_id