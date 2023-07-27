{% macro block_number_by_days_ago(block_table, days_ago) %}
    SELECT
        number
    FROM {{ block_table }}
    WHERE timestamp < dateSub(DAY, {{ days_ago }}, now())
    ORDER BY timestamp DESC
    LIMIT 1
{% endmacro %}

