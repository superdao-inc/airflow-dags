{{
    config(
        materialized = "table",
        engine = 'MergeTree()',
        order_by = "(wallet)",
    )
}}

-- ToDo - move to DBT completely with all intermediates

select address as wallet, eth_balance/1e18 * lp.price_usdt as eth_balance
from {{ source('eth_data_derived', 'eth_balances') }} eb
    , {{ ref('last_prices') }} lp
where 1=1 and  coin_id='eth'