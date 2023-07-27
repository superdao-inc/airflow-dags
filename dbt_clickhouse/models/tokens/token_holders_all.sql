{{
    config(
        materialized = 'table',
        engine = 'MergeTree',
        order_by = '(token_address, wallet)',
        tags = ['wallet_tokens', 'token_holders_all'],
    )
}}

select wallet
    , token_address
    , token_id
    , token_qty
    , 'erc-721' as contract_type
    , 'polygon' as chain
    , 'nft' as token_type
from {{ ref('polygon_wallet_tokens') }} poly_wt

union all
select wallet
    , token_address
    , token_id
    , token_amount as token_qty
    , 'erc-1155' as contract_type
    , 'polygon' as chain
    , 'nft' as token_type
from {{ ref('polygon_wallet_tokens_erc_1155') }} poly_wt

union all
select wallet
    , token_address
    , token_id
    , token_qty
    , 'erc-721' as contract_type
    , 'eth' as chain
    , 'nft' as token_type
from {{ ref('eth_wallet_tokens') }} poly_wt

union all
select wallet
    , token_address
    , token_id
    , token_amount as token_qty
    , 'erc-1155' as contract_type
    , 'eth' as chain
    , 'nft' as token_type
from {{ ref('eth_wallet_tokens_erc_1155') }}