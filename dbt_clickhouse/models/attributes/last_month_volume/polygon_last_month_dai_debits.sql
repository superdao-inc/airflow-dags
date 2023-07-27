{{
  config(
    materialized = "table",
    engine = "MergeTree()",
    order_by = "address",
    tags = ["attr", "polygon", "stablecoins", "attr_last_month_in_volume"]
  )
}}

{{
    generate_fungible_token_debits_query(
        source('polygon_data', 'token_transfers_by_token'),
        source('polygon_data', 'blocks_by_number'),
        '0x8f3cf7ad23cd3cadbd9735aff958023239c6a063',
        30
    )
}}
