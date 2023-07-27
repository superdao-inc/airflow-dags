{% macro generate_native_token_tx_credits_query(native_token_transfers_table) %}

SELECT
    from_address AS address,
    toUInt256(sum(value)) AS value
FROM {{ native_token_transfers_table }}
WHERE from_address IS NOT NULL
GROUP BY from_address

{% endmacro %}