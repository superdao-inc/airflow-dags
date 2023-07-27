{{
    config(
        materialized = "table",
        engine = 'MergeTree()',
        order_by = 'wallet',
    )
}}

{{
    holders_erc_1155(
        source('erc_1155', 'polygon_token_transfers_w_erc1155'),
        source('polygon_data', 'contracts_by_address'),
        'polygon_data',
    )
}}
