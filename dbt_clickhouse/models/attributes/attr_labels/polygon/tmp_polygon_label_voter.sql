{{
    config(
        materialized = "table",
        engine = 'MergeTree()',
        order_by = 'address',
        tags = ['attr_labels_polygon']
    )
}}

SELECT DISTINCT lower(voter) AS address
FROM {{source('raw_data_snapshot', 'votes')}}