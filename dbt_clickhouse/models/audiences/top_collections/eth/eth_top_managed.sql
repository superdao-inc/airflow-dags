{{
   config(
        materialized = "table",
        engine = 'MergeTree()',
        order_by = '(chain, audience_slug, token_address)'
   )
}}


select
     'ETHEREUM'                   as chain
    , aud.audience_slug           as audience_slug
    , min(aud.audience_type)      as audience_type
    , e_wt.token_address          as token_address
    , sum(e_wt.token_qty)         as nft_count
    , count(distinct e_wt.wallet) as holders_count
from {{ ref('eth_wallet_tokens') }} e_wt
    join {{ ref('all_audiences') }} aud on e_wt.wallet = aud.address
group by aud.audience_slug, e_wt.token_address
