{{
  config(
    materialized = "table",
    engine = 'TinyLog()',
    tags = ['report', 'report_defi']
  )
}}

WITH
(
    {{ block_number_by_days_ago(source('eth_data', 'blocks_by_number'), 30) }}
) AS eth_monthly_block_number,

(
    {{ block_number_by_days_ago(source('polygon_data', 'blocks_by_number'), 30) }}
) AS polygon_monthly_block_number,

opensea_interactee AS (
    -- polygon seaport transactions
    SELECT
        from_address AS wallet_address,
        block_number > polygon_monthly_block_number AS monthly
    FROM {{ source('polygon_data', 'transactions_by_to_address') }}
    WHERE to_address = '0x00000000006c3852cbef3e08e8df289169ede581'

    UNION ALL

    -- polygon openstore transfers
    SELECT
        from_address AS wallet_address,
        block_number > polygon_monthly_block_number AS monthly
    FROM {{ source('polygon_data', 'token_transfers_by_token') }}
    WHERE token_address = '0x2953399124f0cbb46d2cbacd8a89cf0599974963'

    UNION ALL

    SELECT
        to_address AS wallet_address,
        block_number > polygon_monthly_block_number AS monthly
    FROM {{ source('polygon_data', 'token_transfers_by_token') }}
    WHERE token_address = '0x2953399124f0cbb46d2cbacd8a89cf0599974963'

    UNION ALL

    -- eth openstore transfers
    SELECT
        from_address AS wallet_address,
        block_number > eth_monthly_block_number AS monthly
    FROM {{ source('eth_data', 'token_transfers_by_token') }}
    WHERE token_address = '0x495f947276749ce646f68ac8c248420045cb7b5e'

    UNION ALL

    SELECT
        to_address AS wallet_address,
        block_number > eth_monthly_block_number AS monthly
    FROM {{ source('eth_data', 'token_transfers_by_token') }}
    WHERE token_address = '0x495f947276749ce646f68ac8c248420045cb7b5e'
),

(
    SELECT
        uniqExact(wallet_address) AS overall_unique
    FROM opensea_interactee
) AS opensea_overall_unique,

(
    SELECT
        uniqExactIf(wallet_address, monthly = 1) AS monthly_unique
    FROM opensea_interactee
) AS opensea_monthly_unique

SELECT
    t.name AS name,
    m.overall_unique AS overall_unique,
    m.monthly_unique AS monthly_unique
FROM VALUES('name String',
    ('OpenSea')
) t
JOIN (
    SELECT
        opensea_overall_unique AS overall_unique,
        opensea_monthly_unique AS monthly_unique,
        'OpenSea' AS name
) m ON t.name = m.name
