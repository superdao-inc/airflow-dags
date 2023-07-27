{{
    config(
        materialized = "table",
        engine = 'MergeTree()',
        order_by = "(wallet,token_address,token_id)",
        tags = ['wallet_tokens', 'top_collections_prerequisites'],
    )
}}

with all_contracts as (
  select address from {{ ref('eth_contracts_by_address') }}
)

SELECT 
  wallet,
  token_address,
  token_id,
  token_qty
from {{ ref('eth_wallet_tokens') }}
where wallet not in all_contracts