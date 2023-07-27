{{
  config(
    materialized = "table",
    engine = "MergeTree()",
    order_by = "address",
    tags = ["attr", "eth", "stablecoins", "attr_last_month_in_volume"]
  )
}}

SELECT
    address,
    sum(value) as last_month_in_volume
FROM (
    SELECT
        address,
        value
    FROM {{ ref('eth_last_month_dai_debits') }}

    UNION ALL

    SELECT
        address,
        value
    FROM {{ ref('eth_last_month_usdc_debits') }}

    UNION ALL

    SELECT
        address,
        value
    FROM {{ ref('eth_last_month_usdt_debits') }}

    UNION ALL

    SELECT
        address,
        value
    FROM {{ ref('eth_last_month_native_debits') }}
)
GROUP BY address