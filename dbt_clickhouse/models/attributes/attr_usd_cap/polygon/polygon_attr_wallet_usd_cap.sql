{{
    config(
        materialized = "table",
        engine = 'MergeTree()',
        order_by = "(address)",
    )
}}

select
    wallet as address
    , toInt64(sum(balance)) as wallet_usd_cap
    , now() as updated
from (
    select wallet, balance
    from {{ ref('polygon_stablecoin_balances') }}

) b
group by wallet
having wallet_usd_cap >= 0