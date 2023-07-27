{{
    config(
        materialized = "table",
        engine = 'MergeTree()',
        order_by = 'address',
        tags = ['attr_labels_polygon']
    )
}}

SELECT address
FROM {{ ref('polygon_attr_wallet_usd_cap') }}
WHERE wallet_usd_cap > 1e5