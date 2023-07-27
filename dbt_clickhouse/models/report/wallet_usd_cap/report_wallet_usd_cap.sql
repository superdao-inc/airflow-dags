{{
    config(
        materialized = "table",
        engine = 'MergeTree()',
        order_by = 'address',
        tags = ['report', 'report_wallet_usd_cap']
    )
}}


select
    address,
    sum(wallet_usd_cap) as wallet_usd_cap
from (
    select
        address,
        coalesce(wallet_usd_cap, 0) as wallet_usd_cap
    from {{ source('eth_data', 'attr_wallet_usd_cap') }}

    union all

    select
        address,
        coalesce(wallet_usd_cap, 0) as wallet_usd_cap
    from {{ source('polygon_data', 'attr_wallet_usd_cap') }}
)
group by address