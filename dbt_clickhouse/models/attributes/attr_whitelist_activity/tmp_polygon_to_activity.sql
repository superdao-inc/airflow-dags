{{
    config(
        materialized = "table",
        engine = 'MergeTree()',
        order_by = 'address',
        tags = ['attr_whitelist_activity_polygon']
    )
}}

SELECT
  DISTINCT transfers.to_address as address,
  transfers.token_address as token_address
FROM {{ ref('polygon_token_transfers_by_to_address') }} as transfers
WHERE transfers.token_address IN polygon_data.whitelist_contracts