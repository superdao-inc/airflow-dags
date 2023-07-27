{{
   config(
        materialized = "table",
        engine = 'MergeTree()',
        order_by = "(token_address, address)",
        tags=['all_claimed_audiences']
   )
}}

select token_address
   , wallet as address
   , 'erc-20' as contract_type
   , 'eth' as chain
from {{ ref('eth_all_claimed_audiences_erc20') }}

union all
select token_address
   , wallet as address
   , 'erc-20' as contract_type
   , 'polygon' as chain
from {{ ref('polygon_all_claimed_audiences_erc20') }}

union all
select token_address
    , wallet as address
    , 'erc-721' as contract_type
    , 'eth' as chain
from {{ ref('eth_all_claimed_audiences_erc721') }}

union all
select token_address
    , wallet as address
    , 'erc-721' as contract_type
    , 'polygon' as chain
from {{ ref('polygon_all_claimed_audiences_erc721') }}

union all
select token_address
    , wallet as address
    , 'erc-1155' as contract_type
    , 'eth' as chain
from {{ ref('eth_all_claimed_audiences_erc1155') }}

union all
select token_address
    , wallet as address
    , 'erc-1155' as contract_type
    , 'polygon' as chain
from {{ ref('polygon_all_claimed_audiences_erc1155') }}