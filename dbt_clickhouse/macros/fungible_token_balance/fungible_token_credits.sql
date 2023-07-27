{% macro
    generate_fungible_token_credits_query(
        transfers_table,
        blocks_table,
        token_address,
        for_last_n_days
    )
-%}

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
    from_address AS address,
    sum(value) AS value
FROM {{ transfers_table }}
WHERE token_address = '{{ token_address }}'
{% if for_last_n_days %}
    AND block_number > monthly_block_number
{% endif %}

GROUP BY address

{% endmacro %}