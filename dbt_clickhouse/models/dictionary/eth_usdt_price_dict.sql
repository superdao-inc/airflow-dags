{{
   config(
        materialized = "table",
        engine = 'MergeTree()',
        order_by = "(key)"
   )
}}


SELECT 
    'last_block_timestamp' as key,
    CAST(max(timestamp) as Int256) as value,
    'INTEGER' as value_type,
    now() as updated
FROM {{ source('eth_data', 'blocks_by_number') }}

UNION ALL

SELECT 
    'eth_price_usd' as key,
    CAST(symbol_price as Int256) as value,
    'INTEGER' as value_type,
    now() as updated
FROM (
        SELECT 
            symbol_price, 
            ROW_NUMBER() OVER(PARTITION BY symbol_id ORDER BY load_timest desc) r
        FROM {{ source('prices', 'binance_prices') }} WHERE symbol_id = 'ETHUSDT') 
WHERE r = 1
