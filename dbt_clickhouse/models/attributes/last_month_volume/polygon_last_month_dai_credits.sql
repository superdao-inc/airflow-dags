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
        source('polygon_raw_data', 'token_transfers'),
        source('polygon_raw_data', 'blocks'),
        "0x8f3cf7ad23cd3cadbd9735aff958023239c6a063",
        30
    )
}}