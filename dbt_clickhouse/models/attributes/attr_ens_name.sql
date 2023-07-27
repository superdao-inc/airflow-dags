{{
  config(
    materialized = "table",
    engine = 'ReplacingMergeTree()',
    order_by = 'address',
    tags = ['attr', 'attr_ens_name']
  )
}}

SELECT
    address,
    ens_name,
    now() as updated
FROM (
    SELECT
        lower(address) AS address,
        ens_name
    FROM {{ source('off_chain', 'pg_ens_name') }}
    WHERE ens_name <> '' AND ens_name IS NOT NULL
    GROUP BY address, ens_name
)