{{
    config(
        materialized = "table",
        engine = 'MergeTree()',
        order_by = "(token_address,wallet,token_id)",
        tags = ['wallet_tokens', 'top_collections_prerequisites'],
    )
}}

with all_contracts as (
  select address from {{ ref('polygon_contracts_by_address') }}
)

SELECT 
  token_address,
  wallet,
  token_id,
  token_qty
from {{ ref('polygon_wallet_tokens') }}
where wallet not in all_contracts