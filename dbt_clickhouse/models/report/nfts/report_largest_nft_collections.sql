{{ config(
    tags = ["market_size_largest_nft_collections"],
    materialized = "table",
    engine = 'MergeTree()',
    order_by = "token_address",
) }} WITH balance as (
    select
        address,
        wallet_usd_cap
    from
        {{ source('eth_data', 'attr_wallet_usd_cap') }}
    union
    all
    select
        address,
        wallet_usd_cap
    from
        {{ source('polygon_data', 'attr_wallet_usd_cap') }}
),
tx_count as (
    select
        address,
        last_month_tx_count
    from
        {{ ref('eth_attr_last_month_tx_count') }}
    union
    all
    select
        address,
        last_month_tx_count
    from
        {{ ref('polygon_attr_last_month_tx_count') }}
)
SELECT
    token_address,
    count(DISTINCT wallet) AS holders_count,
    count(
        DISTINCT CASE
            WHEN balance.wallet_usd_cap >= 100 THEN wallet
        END
    ) AS holders_with_balance_100_or_more,
    count(
        DISTINCT CASE
            WHEN tx_count.last_month_tx_count > 0 THEN wallet
        END
    ) AS holders_with_tx_count_greater_than_0
FROM
    {{ ref('token_holders_all') }}
    LEFT JOIN balance ON balance.address = token_holders_all.wallet
    LEFT JOIN tx_count ON tx_count.address = token_holders_all.wallet
GROUP BY
    token_address
HAVING
    holders_count >= 1 * 100 * 1000
ORDER BY
    holders_count DESC