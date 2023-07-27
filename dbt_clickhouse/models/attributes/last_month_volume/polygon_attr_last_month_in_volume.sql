{{
  config(
    materialized = "table",
    engine = "MergeTree()",
    order_by = "address",
    tags = ["attr", "polygon", "stablecoins", "attr_last_month_in_volume"]
  )
}}

WITH
    1e6 as USDT_DECIMALS,
    1e6 as USDC_DECIMALS,
    1e18 as DAI_DECIMALS,
    1e18 as ETH_DECIMALS

SELECT
    address,
    sum(value) as last_month_in_volume
FROM (
    SELECT
        address,
        toUInt256(value / DAI_DECIMALS) as value
    FROM {{ ref('polygon_last_month_dai_debits') }}

    UNION ALL

    SELECT
        address,
        toUInt256(value / USDC_DECIMALS) as value
    FROM {{ ref('polygon_last_month_usdc_debits') }}

    UNION ALL

    SELECT
        address,
        toUInt256(value / USDT_DECIMALS) as value
    FROM {{ ref('polygon_last_month_usdt_debits') }}
)
GROUP BY address