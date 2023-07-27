{{
    config(
        materialized = "table",
        engine = 'MergeTree()',
        order_by = 'address'
    )
}}

select
    distinct to_address as address
    from {{ source('eth_data', 'transactions_by_to_address') }}
    where not empty(to_address)
