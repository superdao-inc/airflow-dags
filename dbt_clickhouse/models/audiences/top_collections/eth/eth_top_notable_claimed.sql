{{
   config(
        materialized = "table",
        engine = 'MergeTree()',
        order_by = '(base_audience, holded_contract)',
   )
}}

select 
  a.token_address 						as base_audience
  , e1.token_address 					as holded_contract
  , count(distinct a.wallet) 	as holders
  , sum(e1.token_qty) 				as nft_count
from {{ ref('eth_wallet_tokens_without_contracts') }} e1
join (
      select distinct a.token_address, a.wallet
      from {{ ref('eth_wallet_tokens_without_contracts') }} a
  ) a on a.wallet = e1.wallet
where 1 = 1
  and holded_contract in {{ source('eth_data', 'whitelist_contracts') }}
group by a.token_address, e1.token_address
