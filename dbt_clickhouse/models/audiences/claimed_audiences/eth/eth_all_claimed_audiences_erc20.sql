{{
    config(
        materialized = "table",
        engine = 'MergeTree()',
        order_by = 'wallet',
    )
}}

{{
    holders_erc_20(
        source('eth_data', 'contracts_by_address'),
        source('eth_data', 'token_transfers_by_token'),
        source('eth_data', 'input_claimed_audiences'),
        source('eth_data', 'blacklisted_wallets_common')
    )
}}
