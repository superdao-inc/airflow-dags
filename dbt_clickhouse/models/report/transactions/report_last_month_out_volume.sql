{{
  config(
    materialized = "table",
    engine = 'MergeTree()',
    order_by = 'address',
    tags = ['report', 'report_transactions']
  )
}}

SELECT
    address,
    sum(last_month_out_volume) as last_month_out_volume,
    now() as updated
FROM (
    SELECT
        address,
        last_month_out_volume
    FROM {{ ref('eth_attr_last_month_out_volume') }}

    UNION ALL

    SELECT
        address,
        last_month_out_volume
    FROM {{ ref('polygon_attr_last_month_out_volume') }}
)
GROUP BY address