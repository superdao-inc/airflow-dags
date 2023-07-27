{{
    config(
        materialized = "table",
        engine = 'MergeTree()',
        order_by = 'address',
        tags = ['attr_whitelist_activity_polygon']
    )
}}

SELECT address,
  groupUniqArray(token_address) as whitelist_activity,
  now() as updated
FROM (
    SELECT * FROM {{ ref('tmp_polygon_to_activity') }}
    UNION DISTINCT
    SELECT * FROM {{ ref('tmp_polygon_from_activity') }}
    UNION DISTINCT
    SELECT address, '0xdb647193df79ce69b5d34549aae98d519223f682' as token_address
    FROM {{ source('polygon_data', 'farcasters_wallets') }}
  )
GROUP BY address
