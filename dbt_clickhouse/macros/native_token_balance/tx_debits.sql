{% macro generate_native_token_tx_debits_query(native_token_transfers_table) %}

SELECT
    to_address AS address,
    toUInt256(sum(value)) AS value
FROM {{ native_token_transfers_table }}
WHERE to_address IS NOT NULL
GROUP BY to_address

{% endmacro %}