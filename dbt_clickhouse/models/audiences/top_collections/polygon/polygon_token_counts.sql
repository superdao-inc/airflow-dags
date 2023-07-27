{{
   config(
        materialized = "table",
        engine = 'MergeTree()',
        order_by = '(token_address)'
   )
}}

with
all_contracts_polygon as (
	select address from {{ ref('polygon_contracts_by_address') }}
)

select
  token_address,
  count(distinct wallet) as holders,
  count(distinct token_id) as nft_count
from
  {{ ref('polygon_wallet_tokens') }}
where wallet not in all_contracts_polygon
group by token_address
