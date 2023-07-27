{{
  config(
    materialized = "table",
    engine = "MergeTree()",
    order_by = "address",
    tags = ["attr", "eth", "stablecoins", "attr_last_month_out_volume"]
  )
}}

SELECT
    address,
    sum(value) as last_month_out_volume
FROM (
    SELECT
        address,
        value
    FROM {{ ref('eth_last_month_dai_credits') }}

    UNION ALL

    SELECT
        address,
        value
    FROM {{ ref('eth_last_month_usdc_credits') }}

    UNION ALL

    SELECT
        address,
        value
    FROM {{ ref('eth_last_month_usdt_credits') }}

    UNION ALL

    SELECT
        address,
        value
    FROM {{ ref('eth_last_month_native_credits') }}
)
GROUP BY address