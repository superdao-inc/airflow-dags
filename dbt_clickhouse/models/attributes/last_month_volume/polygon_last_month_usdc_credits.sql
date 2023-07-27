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
        "0x2791bca1f2de4661ed88a30c99a7a9449aa84174",
        30
    )
}}
