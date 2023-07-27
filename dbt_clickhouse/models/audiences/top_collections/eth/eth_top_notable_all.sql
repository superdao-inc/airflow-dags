{{
   config(
        materialized = "table",
        engine = 'MergeTree()',
        order_by = '(chain, audience_slug, token_address)'
   )
}}


select 'ETHEREUM'       as chain
    , base_audience     as audience_slug
    , 'CLAIMED'         as audience_type
    , holded_contract   as token_address
    , nft_count
    , holders           as holders_count
from {{ ref('eth_top_notable_claimed') }}

union distinct

select chain
    , audience_slug
    , audience_type
    , token_address
    , nft_count
    , holders_count
from {{ ref('eth_top_managed') }}
where 1=1
    and token_address in {{ source('eth_data', 'whitelist_contracts') }}