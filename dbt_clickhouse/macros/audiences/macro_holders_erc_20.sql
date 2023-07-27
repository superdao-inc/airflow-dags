{% macro holders_erc_20(
    contracts_table,
    transfers_table,
    input_table,
    blacklisted_wallets
) %}

with

fungible_contracts as (
    select distinct collection_contract
    from {{ input_table }}
    where is_fungible=1
),

incomes as (
    SELECT distinct token_address,
        to_address           AS wallet,
        toInt256(sum(value)) AS income
    FROM {{ transfers_table }}
    WHERE token_address IN fungible_contracts
    GROUP BY token_address, to_address
),
outcomes as (
    SELECT distinct token_address,
        from_address         AS wallet,
        toInt256(sum(value)) AS outcome
    FROM {{ transfers_table }}
    WHERE token_address IN fungible_contracts
    GROUP BY token_address, from_address
)
SELECT
    i.wallet,
    i.token_address,
    ( income - coalesce(outcome,0) ) AS balance
FROM incomes i
    left JOIN outcomes o ON i.wallet = o.wallet
    AND i.token_address = o.token_address
WHERE 1=1
    and balance > 0
    and wallet NOT IN ( select c.address from {{ contracts_table }} c )
    and wallet NOT IN {{ blacklisted_wallets }}

{% endmacro %}
