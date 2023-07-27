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
        "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
        30
    )
}}
