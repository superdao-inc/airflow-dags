{{
   config(
        materialized = "table",
        engine = 'MergeTree()',
        order_by = '(chain, audience_slug, token_address)'
   )
}}

select
    chain
    , audience_slug
    , audience_type
    , token_address
    , nft_count
    , holders_count
    , total_eth.nft_count as total_nft_count
    , total_eth.holders   as total_holders_count
    , now() as updated
from {{ ref('eth_top_notable_all') }} all_eth
join {{ ref('eth_token_counts') }} total_eth using (token_address)

union all

select
    chain
    , audience_slug
    , audience_type
    , token_address
    , nft_count
    , holders_count
    , total_polygon.nft_count as total_nft_count
    , total_polygon.holders   as total_holders_count
    , now() as updated
from {{ ref('polygon_top_notable_all') }}
join {{ ref('polygon_token_counts') }} total_polygon using (token_address)