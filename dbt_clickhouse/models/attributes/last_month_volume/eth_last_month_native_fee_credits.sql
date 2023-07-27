{{
  config(
    materialized = "table",
    engine = "MergeTree()",
    order_by = "address",
    tags = ["attr", "eth", "native_token_balance"]
  )
}}

{{
    generate_native_token_fee_credits_query(
      source('eth_data', 'transactions_by_block'),
      source('eth_data', 'blocks_by_number'),
      30
    )
}}
