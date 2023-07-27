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
(
    -- usdt
    '0xdac17f958d2ee523a2206206994597c13d831ec7',
    -- usdc
    '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
    -- dai
    '0x6b175474e89094c44da98b954eedeac495271d0f',
    -- busd
    '0x4fabb145d64652a948d72533023f6e7a623c7c53',
    -- tusd
    '0x0000000000085d4780b73119b644ae5ecd22b376'
) AS eth_stablecoins_contracts,
(
    -- usdt
    '0xc2132d05d31c914a87c6611c10748aeb04b58e8f',
    -- usdc
    '0x2791bca1f2de4661ed88a30c99a7a9449aa84174',
    -- dai
    '0x8f3cf7ad23cd3cadbd9735aff958023239c6a063',
    -- busd
    '0x9c9e5fd8bbc25984b178fdce6117defa39d2db39',
    -- tusd
    '0x2e1ad108ff1d8c782fcbbb89aad783ac49586756'
) AS polygon_stablecoins_contracts

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
        AND token_address IN eth_stablecoins_contracts

    UNION ALL

    -- eth incoming
    SELECT
        token_address,
        to_address AS address,
        'eth' AS chain,
        'incoming' AS direction
    FROM {{ source('eth_data', 'token_transfers_by_token') }}
    WHERE block_number >= eth_monthly_block_number
        AND token_address IN eth_stablecoins_contracts

    UNION ALL

    -- polygon outgoing
    SELECT
        token_address,
        from_address AS address,
        'polygon' AS chain,
        'outgoing' AS direction
    FROM {{ source('polygon_data', 'token_transfers_by_token') }}
    WHERE block_number >= polygon_monthly_block_number
        AND token_address IN polygon_stablecoins_contracts

    UNION ALL

    -- polygon incoming
    SELECT
        token_address,
        to_address AS address,
        'polygon' AS chain,
        'incoming' AS direction
    FROM {{ source('polygon_data', 'token_transfers_by_token') }}
    WHERE block_number >= polygon_monthly_block_number
        AND token_address IN polygon_stablecoins_contracts
)
