{{
  config(
    materialized = "table",
    engine = "MergeTree()",
    order_by = "from_address, to_address",
    tags = ["attr", "eth", "native_token_balance"]
  )
}}

{{
    generate_native_token_transfers_query(
        source('eth_data', 'traces_by_block'),
        source('eth_data', 'blocks_by_number'),
        30
    )
}}
