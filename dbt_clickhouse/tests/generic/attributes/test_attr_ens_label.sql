{% test test_attr_ens_label(model, column_name, compare_with_table) %}

SELECT
    (
        SELECT count(*) FROM {{model}}
        WHERE has({{column_name}}, 'ens')
    ) AS eth_data_count,

    (
        SELECT count(*) FROM {{compare_with_table}}
    ) AS off_chain_count

WHERE eth_data_count > off_chain_count
    AND eth_data_count < (off_chain_count * 0.9)

{% endtest %}

