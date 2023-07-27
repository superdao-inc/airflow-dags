{{
  config(
    materialized = "table",
    engine = 'MergeTree()',
    order_by = 'address',
    tags = ['attr', 'polygon', 'attr_tx_count']
  )
}}

SELECT
    from_address AS address,
    count(*) AS tx_count,
    now() as updated
FROM {{ source('polygon_data', 'transactions_by_from_address') }}
GROUP BY from_address
