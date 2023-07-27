{%test test_attr_whale_label(model, column_name, compare_with_table) %}

SELECT
    (
        SELECT count(*) FROM {{model}}
        WHERE has({{column_name}}, 'whale')
    ) AS whale_count,

    (
        SELECT count(*) FROM {{compare_with_table}}
        WHERE wallet_usd_cap >= 1e5
    ) as compare_count

WHERE whale_count > compare_count
    AND whale_count < (compare_count * 0.9)

{% endtest %}