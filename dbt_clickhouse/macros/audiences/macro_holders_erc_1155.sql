{% macro holders_erc_1155(
    transfers_table,
    contract_table,
    source_id
) %}

with incomes as (
    select token_address
        , to_address as wallet
        , token_id
        , toInt256(sum(value)) AS income
    from {{ transfers_table }} tr
    where 1=1
        and tr.value>0
        and tr.token_id > 0
        and token_address IN (
                    select collection_contract
                    from {{ source(source_id, 'input_claimed_audiences') }}
                    where is_fungible
                    )
    GROUP BY token_address, to_address, token_id
), outcomes as (
    select token_address
        , from_address as wallet
        , token_id
        , toInt256(sum(value)) AS outcome
    from {{ transfers_table }} tr
    where 1=1
        and tr.value>0
        and tr.token_id > 0
        and token_address IN (
                    select collection_contract
                    from {{ source(source_id, 'input_claimed_audiences') }}
                    where is_fungible
                    )
    GROUP BY token_address, from_address, token_id
)
SELECT
    wallet,
    token_address,
    (income-outcome) AS balance
FROM incomes i
    FULL JOIN outcomes o ON i.wallet = o.wallet
WHERE balance > 0
    AND wallet NOT IN ( select address from {{ contract_table }} )
    AND wallet NOT IN {{ source(source_id, 'blacklisted_wallets_common') }}

{% endmacro %}