{{
  config(
    materialized = "table",
    engine = "MergeTree()",
    order_by = "address",
    tags = ["attr", "eth", "stablecoins", "attr_last_month_out_volume"]
  )
}}

WITH
    1e6 as USDT_DECIMALS,
    1e6 as USDC_DECIMALS,
    1e18 as DAI_DECIMALS,
    1e18 as ETH_DECIMALS,
    (
        SELECT toUInt256(symbol_price)
        FROM {{ source('prices', 'binance_prices') }}
        WHERE symbol_id = 'ETHUSDT'
        ORDER BY load_timest DESC
        LIMIT 1
    ) as ETH_TO_USD

SELECT
    address,
    sum(value) as last_month_out_volume
FROM (
    SELECT
        address,
        toUInt256(value / DAI_DECIMALS) as value
    FROM {{ ref('eth_last_month_dai_credits') }}

    UNION ALL

    SELECT
        address,
        toUInt256(value / USDC_DECIMALS) as value
    FROM {{ ref('eth_last_month_usdc_credits') }}

    UNION ALL

    SELECT
        address,
        toUInt256(value / USDT_DECIMALS) as value
    FROM {{ ref('eth_last_month_usdt_credits') }}

    UNION ALL

    SELECT
        address,
        toUInt256(toFloat64(value * ETH_TO_USD) / ETH_DECIMALS) as value
    FROM {{ ref('eth_last_month_native_credits') }}
)
WHERE address <> ''
GROUP BY address