{{
    config(
        materialized = "table",
        engine = 'MergeTree()',
        order_by = 'wallet',
    )
}}

{{
    holders_erc_721(
        ref('polygon_wallet_tokens'),
        source('polygon_data', 'contracts_by_address'),
        'polygon_data',
    )
}}
