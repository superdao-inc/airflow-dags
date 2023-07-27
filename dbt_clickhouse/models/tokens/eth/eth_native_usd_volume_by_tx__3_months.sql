{{
    config(
        materialized = "table",
        engine = 'MergeTree()',
        order_by = 'tx_hash',
        tags = ['tx_volumes']
    )
}}

with (
    select symbol_price
    from {{ source('prices', 'binance_prices') }}
    where symbol_id = 'ETHUSDT'
    order by load_timest desc
    limit 1
) as eth_usdt_rate,
(
  select number
  from {{ source('eth_data', 'blocks_by_number') }}
  where timestamp < dateSub(DAY, 90, now())
  order by timestamp desc
  limit 1
) as three_month_ago,
1e18 as eth_decimals

select
    transactions.hash as tx_hash,
    round(eth_usdt_rate * (transactions.value/eth_decimals), 2) as volume
from
    {{ source('eth_data', 'transactions_by_block') }} transactions
where
    transactions.block_number >= three_month_ago
    and transactions.value > 0
