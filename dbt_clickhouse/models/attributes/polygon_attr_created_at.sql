{{
  config(
    materialized='table',
    unique_key='address',
    engine = 'MergeTree()',
    order_by = 'address',
    tags = ['attr', 'polygon', 'attr_created_at']
  )
}}


SELECT distinct
    address,
    blocks.timestamp AS created_at,
    now() AS updated
FROM (
    SELECT
        from_address AS address,
        min(block_number) AS min_block_number
    FROM {{ source('polygon_data_derived', 'transactions_by_from_address') }}
    GROUP BY address
) AS t
JOIN {{ source('polygon_data', 'blocks_by_number') }} AS blocks ON (t.min_block_number = blocks.number)
