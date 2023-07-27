{{
    config(
        materialized = "table",
        engine = 'MergeTree()',
        order_by = 'address',
        tags = ['attr_labels_common']
    )
}}

SELECT
    address,
    count(1) as ens_count
FROM {{source('off_chain', 'attr_ens_name')}}
GROUP BY address, ens_name
HAVING ens_count > 0
