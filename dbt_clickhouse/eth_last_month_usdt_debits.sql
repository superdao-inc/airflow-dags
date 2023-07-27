{{
  config(
    materialized = "table",
    engine = "MergeTree()",
    order_by = "address",
    tags = ["attr", "eth", "stablecoins", "attr_last_month_in_volume"]
  )
}}

{{
    generate_fungible_token_debits_query(
        source('eth_data', 'token_transfers_by_token'),
        source('eth_data', 'blocks_by_number'),
        '0xdac17f958d2ee523a2206206994597c13d831ec7',
        30
    )
}}
