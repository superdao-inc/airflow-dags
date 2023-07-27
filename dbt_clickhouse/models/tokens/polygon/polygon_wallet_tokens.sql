{{
   config(
        materialized = "incremental",
        engine = 'MergeTree()',
        order_by = "(wallet,token_address,token_id)",
		incremental_strategy = "append",
        tags = ['wallet_tokens'],
   )
}}

{% set token_buckets = var("token_buckets", []) %}

SELECT wallet, token_address, token_id, 1 AS token_qty
FROM (
	SELECT
		transfers.token_address AS token_address,
		transfers.value AS token_id,
		last_value(transfers.to_address) OVER w AS wallet
	FROM {{ source('polygon_data', 'token_transfers_by_token') }} AS transfers
	JOIN {{ ref('polygon_contracts_by_address') }} contracts
	ON transfers.token_address = contracts.address
	AND contracts.is_erc20 = false
	WHERE 1=1
	{% if token_buckets %}
		AND (
			{% for bucket in token_buckets %}
				startsWith(transfers.token_address, '{{ bucket }}')
				{% if not loop.last %} OR {% endif %}
			{% endfor %}
		)
	{% endif %}
	WINDOW w AS (
		PARTITION BY transfers.token_address, transfers.value
		ORDER BY transfers.block_number, transfers.log_index
		RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
	)
)
WHERE wallet NOT IN {{ source('polygon_data', 'blacklisted_wallets_common') }}
GROUP BY wallet, token_address, token_id
