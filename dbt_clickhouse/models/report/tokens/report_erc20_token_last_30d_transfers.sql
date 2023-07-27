{{
  config(
    materialized = "table",
    engine = 'MergeTree()',
    order_by = 'address',
    tags = ['report', 'report_tokens']
  )
}}

WITH
(
    {{ block_number_by_days_ago(source('eth_data', 'blocks_by_number'), 30) }}
) AS eth_monthly_block_number,
(
    {{ block_number_by_days_ago(source('polygon_data', 'blocks_by_number'), 30) }}
) AS polygon_monthly_block_number,
eth_erc20_contracts AS (
    SELECT address AS contract
    FROM {{ source('eth_data', 'contracts_by_address') }}
    WHERE is_erc20=true
),
polygon_erc20_contracts AS (
    SELECT address AS contract
    FROM {{ source('polygon_data', 'contracts_by_address') }}
    WHERE is_erc20=true
)

SELECT *
FROM (
    -- eth outgoing
    SELECT
        token_address,
        from_address AS address,
        'eth' AS chain,
        'outgoing' AS direction
    FROM {{ source('eth_data', 'token_transfers_by_token') }}
    WHERE block_number >= eth_monthly_block_number
        AND token_address IN eth_erc20_contracts

    UNION ALL

    -- eth incoming
    SELECT
        token_address,
        to_address AS address,
        'eth' AS chain,
        'incoming' AS direction
    FROM {{ source('eth_data', 'token_transfers_by_token') }}
    WHERE block_number >= eth_monthly_block_number
        AND token_address IN eth_erc20_contracts

    UNION ALL

    -- polygon outgoing
    SELECT
        token_address,
        from_address AS address,
        'polygon' AS chain,
        'outgoing' AS direction
    FROM {{ source('polygon_data', 'token_transfers_by_token') }}
    WHERE block_number >= polygon_monthly_block_number
        AND token_address IN polygon_erc20_contracts

    UNION ALL

    -- polygon incoming
    SELECT
        token_address,
        to_address AS address,
        'polygon' AS chain,
        'incoming' AS direction
    FROM {{ source('polygon_data', 'token_transfers_by_token') }}
    WHERE block_number >= polygon_monthly_block_number
        AND token_address IN polygon_erc20_contracts
)
