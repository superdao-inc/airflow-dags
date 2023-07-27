{% macro generate_native_token_transfers_query(traces_table, blocks_table, for_last_n_days) %}

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
    from_address,
    to_address,
    value,
    block_number
FROM {{ traces_table }}
WHERE (status = 1)
    AND ((call_type NOT IN ('delegatecall', 'callcode', 'staticcall')) OR (call_type IS NULL))
    {% if for_last_n_days %}
    AND block_number > monthly_block_number
    {% endif %}

{% endmacro %}