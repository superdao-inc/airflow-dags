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
    from {{ ref('eth_stablecoin_balances') }}

    union all
    select wallet, eth_balance
    from {{ ref('eth_basecoin_balances') }} eth
) b
group by wallet
having wallet_usd_cap >= 0