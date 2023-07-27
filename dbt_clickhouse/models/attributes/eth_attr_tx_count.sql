{{
  config(
    materialized = "table",
    engine = 'MergeTree()',
    order_by = 'address',
    tags = ['attr', 'eth', 'attr_tx_count']
  )
}}

SELECT
    from_address AS address,
    count(*) AS tx_count,
    now() as updated
FROM {{ source('eth_data', 'transactions_by_from_address') }}
GROUP BY from_address
