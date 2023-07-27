{{
  config(
    materialized = "table",
    engine = "MergeTree()",
    order_by = "address",
    tags = ["attr", "eth", "stablecoins", "attr_last_month_out_volume"]
  )
}}

{{
    generate_fungible_token_credits_query(
        source('eth_data', 'token_transfers_by_token'),
        source('eth_data', 'blocks_by_number'),
        "0x6b175474e89094c44da98b954eedeac495271d0f",
        30
    )
}}
