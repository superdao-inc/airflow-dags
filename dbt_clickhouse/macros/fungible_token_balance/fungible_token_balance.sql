{% macro generate_fungible_token_balance_query(token_name) -%}

SELECT
    address,
    sum(balance) as balance
FROM (
    SELECT
        address,
        -toInt256(value) as balance
    FROM {{ chain_db }}.{{ token_name }}_token_credits

    UNION ALL

    SELECT
        address,
        toInt256(value) as balance
    FROM {{ chain_db }}.{{ token_name }}_token_debits
)
GROUP BY address

{% endmacro %}