{{
    config(
        materialized = "table",
        engine = 'MergeTree()',
        order_by = 'address'
    )
}}

SELECT
    address,
    sum(nfts_count) as nfts_count,
    now() as updated
FROM {{ ref('polygon_nfts_count_detailed') }}
group by address