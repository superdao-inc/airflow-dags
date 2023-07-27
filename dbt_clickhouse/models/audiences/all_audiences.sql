{{
   config(
        materialized = "table",
        engine = 'MergeTree()',
        order_by = "(address, audience_slug)",
   )
}}


----- Macro audiences
select ws.score_id as audience_slug
    , 'AUDIENCE' as audience_type
    , 'ethereum' as chain
    , ws.wallet as address
    -- , updated
from {{ source('eth_data_derived', 'wallet_score') }} ws
group by ws.score_id, ws.wallet

union all

select ws.score_id as audience_slug
   , 'AUDIENCE' as audience_type
   , 'POLYGON' as chain
   , ws.wallet as address
   -- , updated
from {{ source('polygon_data_derived', 'wallet_score') }} ws
group by ws.score_id, ws.wallet

union all

----- Claimed audiences
select token_address as audience_slug
    , 'CLAIMED' as audience_type
    , chain
    , address
from {{ ref('all_claimed_audiences') }}

union all

select wle.tracker_id as audience_slug
    , 'ANALYTICS' as audience_type
    , 'cross-chain' as chain
    , wle.address
    -- , updated
from {{ source('analytics', 'wallet_last_events') }} wle
group by wle.tracker_id, wle.address

union all

select
    distinct
    fl.list_id as audience_slug
    , 'FIXED_LIST' as audience_type
    , 'cross-chain' as chain
    , fl.wallet
from {{ source('audiences', 'scoring_api_fixed_lists') }} fl