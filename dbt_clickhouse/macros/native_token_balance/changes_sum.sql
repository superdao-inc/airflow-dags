{% macro generate_native_token_changes_sum_query(fees_amount, tx_amount) %}

WITH
fees_amount_sum AS (
    SELECT
        address,
        sum(value) AS value
    FROM {{ fees_amount }}
    GROUP BY address
),
tx_amount_sum AS (
    SELECT
        address,
        sum(value) AS value
    FROM {{ tx_amount }}
    GROUP BY address
)

SELECT
    address,
    sum(value) AS value
FROM (
    SELECT *
    FROM fees_amount_sum
    UNION ALL
    SELECT *
    FROM tx_amount_sum
)
GROUP BY address

{% endmacro %}