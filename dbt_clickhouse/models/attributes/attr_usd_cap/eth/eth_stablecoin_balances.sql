{{
   config(
        materialized = "table",
        engine = 'MergeTree()',
        order_by = "(wallet,token_address)",
   )
}}


with cr as (
	select to_address as wallet
		, t.token_address
		, sum(value/decimals) as credits
	from {{ source('eth_data', 'token_transfers_by_token') }} t
		join {{ ref('dict_stablecoins') }} st on t.token_address=st.token_contract
			and st.chain = 'eth'
	group by to_address, t.token_address
), db as (
	select from_address as wallet
		, t.token_address
		, sum(value/decimals) as debits
	from {{ source('eth_data', 'token_transfers_by_token') }} t
		join {{ ref('dict_stablecoins') }} st on t.token_address=st.token_contract
			and st.chain = 'eth'
	group by from_address, t.token_address
)
select cr.wallet
    , cr.token_address
	, credits - coalesce(debits,0) as balance
from cr
	left join db on cr.wallet = db.wallet and cr.token_address = db.token_address
where balance > 0
