{{
    config(
        materialized = "table",
        engine = 'MergeTree()',
        order_by = 'wallet',
    )
}}

{{
    holders_erc_1155(
        source('erc_1155', 'token_transfers_w_erc1155'),
        source('eth_data', 'contracts_by_address'),
        'eth_data',
    )
}}
