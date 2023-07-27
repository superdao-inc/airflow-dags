{{
  config(
    materialized = "table",
    engine = "MergeTree()",
    order_by = "address",
    tags = ["attr", "polygon", "stablecoins", "attr_last_month_out_volume"]
  )
}}

{{
    generate_fungible_token_credits_query(
        source('polygon_data', 'token_transfers_by_token'),
        source('polygon_data', 'blocks_by_number'),
        "0xc2132d05d31c914a87c6611c10748aeb04b58e8f",
        30
    )
}}
