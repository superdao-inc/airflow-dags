{{
    config(
        materialized = "table",
        engine = 'MergeTree()',
        order_by = 'address'
    )
}}

with (
    {{ block_number_by_days_ago(source('polygon_data', 'blocks_by_number'), 90) }}
) as three_month_ago,

native_token_transfers as (
    select
        transactions.hash as tx_hash
    from
        {{ source('polygon_data', 'transactions_by_block') }} transactions
    where 
        transactions.block_number >= three_month_ago
        and transactions.value > 0
),

valuable_transactions as (
    select
        native_token_transfers.tx_hash as tx_hash
    from native_token_transfers

    union distinct
    
    select
        erc20_transfers.tx_hash as tx_hash
    from {{ ref('polygon_tx_stablecoins__3_months') }} erc20_transfers
)

select
    transfers.from_address as address,
    transfers.token_address as token,
    transfers.value as value
from {{ source('polygon_data', 'token_transfers_by_token') }} transfers
inner join {{ ref('polygon_contracts_by_address') }} contracts
    on transfers.token_address = contracts.address
    and contracts.is_erc721 = true
inner join valuable_transactions valuable_transactions
    on transfers.transaction_hash = valuable_transactions.tx_hash
where
    transfers.from_address not in [
        '0x0000000000000000000000000000000000000000',
        '0x000000000000000000000000000000000000dead'
    ]
    and transfers.block_number >= three_month_ago
