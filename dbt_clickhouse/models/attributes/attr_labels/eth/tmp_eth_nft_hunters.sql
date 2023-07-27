{{
    config(
        materialized = "table",
        engine = 'MergeTree()',
        order_by = 'address',
        tags = ['attr_labels_prerequisites']
    )
}}

WITH
    all_contracts AS (
        SELECT DISTINCT address FROM {{ source('eth_data', 'contracts_by_address') }}
    ),
    erc721_contracts AS (
        SELECT
            address AS contract
        FROM {{ source('eth_data', 'contracts_by_address')}}
        WHERE is_erc721=true
    ),
    erc721_income_count AS (
        SELECT
            to_address AS wallet,
            count(to_address) AS income_count
        FROM {{ source('eth_data', 'token_transfers_by_token') }}
        WHERE token_address IN erc721_contracts
        GROUP BY to_address
    ),
    erc721_outcome_count AS (
        SELECT
            from_address AS wallet,
            count(from_address) AS outcome_count
        FROM {{ source('eth_data', 'token_transfers_by_token') }}
        WHERE token_address IN erc721_contracts
        GROUP BY from_address
    )

SELECT
    erc721_income_count.wallet AS address
FROM erc721_income_count
JOIN erc721_outcome_count ON erc721_income_count.wallet = erc721_outcome_count.wallet

WHERE
    (income_count - outcome_count) / outcome_count < 0.75
    AND income_count > 1000
    AND income_count >= outcome_count
    AND address NOT IN all_contracts
