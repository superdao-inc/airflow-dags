{{
  config(
    materialized = "table",
    engine = 'MergeTree()',
    order_by = 'address',
    tags = ['report', 'report_nfts']
  )
}}

SELECT
    address,
    sum(nfts_count) as nfts_count,
    now() as updated
FROM (
    SELECT
        address,
        nfts_count
    FROM {{ ref('eth_attr_nfts_count') }}

    UNION ALL

    SELECT
        address,
        nfts_count
    FROM {{ ref('polygon_attr_nfts_count') }}
)
GROUP BY address