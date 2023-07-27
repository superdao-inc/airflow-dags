{{
    config(
        materialized = 'table',
        engine = 'ReplacingMergeTree(token_amount)',
        order_by = '(wallet, token_address, token_id)',
        incremental_strategy = "append",
    )
}}

with t as (
    select tt.token_address
            , tt.to_address as wallet
            , tt.token_id
            , toInt256(sum(tt.value)) as token_amount
            , max(tt.block_number) as block_number
    from {{ source('erc_1155', 'polygon_token_transfers_w_erc1155') }} tt
    prewhere substr(token_address,1,3) = '{{ var("token_prefix", "0x1") }}'
    where 1=1
        and token_id <> 0
        and token_address not in (
            select address
            from {{ source('polygon_data', 'contracts_by_address') }}
            where (is_erc20 or is_erc721)
        )

        {% if is_incremental() %}
        and tt.block_number >= ( select max(block_number) from {{this}} )
        {% endif %}

    group by tt.token_address, tt.to_address, tt.token_id

    union all
    select tt.token_address
            , tt.from_address as wallet
            , tt.token_id
            , toInt256(-sum(tt.value)) as token_amount
            , max(tt.block_number) as block_number
    from {{ source('erc_1155', 'polygon_token_transfers_w_erc1155') }} tt
    prewhere substr(token_address,1,3) = '{{ var("token_prefix", "0x1") }}'
    where 1=1
        and token_id <> 0
        and token_address not in (
            select address
            from {{ source('polygon_data', 'contracts_by_address') }}
            where (is_erc20 or is_erc721)
        )

        {% if is_incremental() %}
        and tt.block_number >= ( select max(block_number) from {{this}} )
        {% endif %}

    group by tt.token_address, tt.from_address, tt.token_id
)
select token_address, token_id, wallet
    , sum(token_amount) as token_amount
    , max(t.block_number) as block_number
from t
group by token_address, token_id, wallet
