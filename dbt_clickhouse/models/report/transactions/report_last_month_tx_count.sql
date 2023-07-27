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
    sum(last_month_tx_count) as last_month_tx_count,
    now() as updated
FROM (
    SELECT
        address,
        last_month_tx_count
    FROM {{ ref('eth_attr_last_month_tx_count') }}

    UNION ALL

    SELECT
        address,
        last_month_tx_count
    FROM {{ ref('polygon_attr_last_month_tx_count') }}
)
GROUP BY address