{% macro generate_native_token_fee_debits_query(transactions_table, blocks_table, for_last_n_days) %}

{% if for_last_n_days %}
WITH (
    SELECT number
    FROM {{ blocks_table }}
    WHERE timestamp < now() - interval {{ for_last_n_days }} day
    ORDER BY number DESC
    LIMIT 1
) as monthly_block_number
{% endif %}

SELECT
    miner AS address,
    toUInt256(sum(receipt_gas_used * gas_price)) AS value
FROM {{ transactions_table }} AS transactions
INNER JOIN {{ blocks_table }} AS blocks ON transactions.block_number = blocks.number
GROUP BY address

{% endmacro %}