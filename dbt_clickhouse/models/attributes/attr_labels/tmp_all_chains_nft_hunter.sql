{{
    config(
        materialized = "table",
        engine = 'MergeTree()',
        order_by = 'address',
        tags = ['attr_labels_prerequisites']
    )
}}

with
    total_usd_cap_1000_or_less as (
        select
            address,
            coalesce(eth.wallet_usd_cap, 0) + coalesce(poly.wallet_usd_cap, 0) as balance
        from
            {{ ref('eth_attr_wallet_usd_cap') }} eth
        full outer join
            {{ ref('polygon_attr_wallet_usd_cap') }} poly using (address)
        where
            balance <= 1000
    )

select
    distinct address
from (
    select address from {{ ref('tmp_eth_nft_hunters') }}
    union all 
    select address from {{ ref('tmp_polygon_nft_hunters') }}
) hunters
join total_usd_cap_1000_or_less balances using (address)
where address not in (
    select address from {{ ref('tmp_eth_label_influencer') }}
    union all
    select address from {{ ref('tmp_polygon_label_influencer') }}
)
