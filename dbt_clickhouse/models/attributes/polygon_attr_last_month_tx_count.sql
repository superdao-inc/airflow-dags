{{
  config(
    materialized = "table",
    engine = 'ReplacingMergeTree()',
    order_by = 'address',
    tags = ['attr', 'polygon', 'attr_last_month_tx_count']
  )
}}


WITH (
    SELECT number
    FROM {{ source('polygon_data', 'blocks_by_number') }}
    WHERE timestamp < now() - interval 30 day
    ORDER BY number DESC
    LIMIT 1
) as monthly_block_number

SELECT
    from_address AS address,
    count(transaction_index) AS last_month_tx_count,
    now() AS updated
FROM {{ source('polygon_data', 'transactions_by_block') }}
WHERE block_number > monthly_block_number
GROUP BY from_address
