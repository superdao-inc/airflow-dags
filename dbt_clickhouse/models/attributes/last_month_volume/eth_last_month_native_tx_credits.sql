{{
  config(
    materialized = "table",
    engine = "MergeTree()",
    order_by = "address",
    tags = ["attr", "eth", "native_token_balance"]
  )
}}

{{
    generate_native_token_tx_credits_query(ref('eth_last_month_native_transfers'))
}}