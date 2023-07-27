{{
    config(
        materialized = "table",
        engine = 'MergeTree()',
        order_by = 'wallet',
    )
}}

{{
    holders_erc_721(
        ref('eth_wallet_tokens'),
        source('eth_data', 'contracts_by_address'),
        'eth_data',
    )
}}
