{{
    config(
        materialized = "table",
        engine = 'MergeTree()',
        order_by = 'tx_hash'
    )
}}

{{
    stablecoins_transfers_transactions(
        source('polygon_data', 'token_transfers_by_token'),
        source('polygon_data', 'blocks_by_number'),
        90
    )
}}
