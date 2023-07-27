{{
    config(
        materialized = "incremental",
        engine = 'MergeTree()',
        order_by = '(wallet, token_address, token_id)',
        incremental_strategy = "append",
    )
}}

select t.token_address, t.token_id, t.wallet, t.token_amount
    , block_number
from (
    select th.token_address, th.token_id, th.wallet, th.token_amount
        , sum(th.token_amount) over (partition by th.token_address, th.token_id) as total_token_amount
        , th.block_number
    from {{ ref('eth_token_holders_erc_1155') }} th
    where 1=1
        and wallet not in ('0x00000000')
        and wallet <> token_address
        and token_amount > 0

        {% if is_incremental() %}
        and th.block_number >= ( select max(block_number) from {{this}} )
        {% endif %}
) t

where 1=1
    -- first filter :: more than 1 token holded on 1 wallet = FT
    and total_token_amount = 1
    -- second filer :: more then 1 token holded on multiple wallets
    and t.token_amount = 1