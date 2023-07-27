{{
    config(
        materialized = "table",
        engine = 'MergeTree()',
        order_by = 'address',
        tags = ['attr_whitelist_activity_eth']
    )
}}

SELECT 
  DISTINCT transfers.from_address as address,
  transfers.token_address as token_address
FROM {{ ref('eth_token_transfers_by_from_address') }} as transfers
WHERE transfers.token_address IN eth_data.whitelist_contracts