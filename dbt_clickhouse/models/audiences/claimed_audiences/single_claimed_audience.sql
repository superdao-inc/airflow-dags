{{
   config(
        materialized = "table",
        engine = 'MergeTree()',
        order_by = "wallet",
   )
}}

WITH
all_contracts_subquery as (
    SELECT DISTINCT address
    FROM {{ var("ch_database", "eth_data") }}.contracts_by_address
),

base_audience_subquery as (
    SELECT
        holder as wallet,
        block_number,
        token_address
    from (
        SELECT
            holder,
            block_number,
            token_address,
            row_number() OVER (PARTITION BY holder ORDER BY block_number, log_index) AS rn
            FROM (
                SELECT
                    last_value(transfers.to_address) OVER w AS holder,
                    block_number,
                    log_index,
                    transfers.token_address
                FROM {{ var('ch_database', 'eth_data') }}.token_transfers_by_token AS transfers
                WHERE transfers.token_address = '{{ var("collection_contract", "Null") }}'
                WINDOW w AS (
                    PARTITION BY transfers.token_address, transfers.value
                    ORDER BY transfers.block_number, transfers.log_index
                    RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                )
            )
        )
    WHERE rn = 1
        AND wallet NOT IN all_contracts_subquery
        AND wallet NOT IN {{ var('ch_database', 'eth_data') }}.blacklisted_wallets_common
),

base_audience_subquery_fungible as (
    SELECT
        wallet,
        token_address,
        NULL             AS block_number,
        (income-outcome) AS balance
    FROM (
            SELECT token_address,
                to_address           AS wallet,
                toInt256(sum(value)) AS income
            FROM {{ var('ch_database', 'eth_data') }}.token_transfers_by_token
            WHERE token_address = '{{ var("collection_contract", "Null") }}'
            GROUP BY token_address, to_address
    ) incomes
    FULL JOIN (
            SELECT token_address,
                from_address         AS wallet,
                toInt256(sum(value)) AS outcome
            FROM {{ var('ch_database', 'eth_data') }}.token_transfers_by_token
            WHERE token_address = '{{ var("collection_contract", "Null") }}'
            GROUP BY token_address, from_address
    ) outcomes ON incomes.wallet = outcomes.wallet
    WHERE balance > 0
        AND wallet NOT IN all_contracts_subquery
        AND wallet NOT IN {{ var('ch_database', 'eth_data') }}.blacklisted_wallets_common
)

SELECT DISTINCT
    wallet,
    {{ "'eth'" if var('ch_database', 'eth_data') == 'eth_data' else "'polygon'" }} as chain,
    token_address

FROM (
    SELECT
        base_query.wallet as wallet,
        base_query.token_address as token_address

    FROM
        {{ 'base_audience_subquery_fungible' if var('is_fungible', 'true') == 'true' else 'base_audience_subquery' }}
    as base_query
)
