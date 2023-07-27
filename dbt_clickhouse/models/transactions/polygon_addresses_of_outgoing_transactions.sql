{{
    config(
        materialized = "table",
        engine = 'MergeTree()',
        order_by = 'block_number'
    )
}}

select
    from_address as address,
    toUInt256(max(block_number)) as block_number
    from {{ source('polygon_data', 'transactions_by_from_address') }}
group by from_address

union distinct

select
    from_address as address,
    toUInt256(max(block_number)) as block_number
    from {{ ref('polygon_token_transfers_by_from_address') }}
group by from_address
