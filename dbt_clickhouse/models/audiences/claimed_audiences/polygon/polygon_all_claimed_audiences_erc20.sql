{{
    config(
        materialized = "table",
        engine = 'MergeTree()',
        order_by = 'wallet',
    )
}}

{{
    holders_erc_20(
        source('polygon_data', 'contracts_by_address'),
        source('polygon_data', 'token_transfers_by_token'),
        source('polygon_data', 'input_claimed_audiences'),
        source('polygon_data', 'blacklisted_wallets_common')
    )
}}
