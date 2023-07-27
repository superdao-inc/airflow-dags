{{
    config(
        materialized = "table",
        engine = 'MergeTree()',
        order_by = 'address',
        tags = ['report', 'report_wallets']
    )
}}

SELECT address
FROM (
    -- eth transfers
    SELECT to_address AS address
    FROM {{ source('eth_data', 'transactions_by_to_address') }}

    UNION DISTINCT

    SELECT from_address AS address
    FROM {{ source('eth_data', 'transactions_by_from_address') }}

    UNION DISTINCT

    -- eth transactions
    SELECT to_address AS address
    FROM {{ source('eth_data', 'transactions_by_to_address') }}

    UNION DISTINCT

    SELECT from_address AS address
    FROM {{ source('eth_data', 'transactions_by_from_address') }}

    UNION DISTINCT

    -- polygon transfers
    SELECT to_address AS address
    FROM {{ ref('polygon_token_transfers_by_to_address') }}

    UNION DISTINCT

    SELECT from_address AS address
    FROM {{ ref('polygon_token_transfers_by_from_address') }}

    UNION DISTINCT

    -- polygon transactions
    SELECT to_address AS address
    FROM {{ source('polygon_raw_data', 'transactions') }}

    UNION DISTINCT

    SELECT from_address AS address
    FROM {{ source('polygon_raw_data', 'transactions') }}
)
WHERE address NOT IN [
        '',
        '0x0000000000000000000000000000000000000000',
        '0x000000000000000000000000000000000000dead'
    ]
