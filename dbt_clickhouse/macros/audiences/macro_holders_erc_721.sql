{% macro holders_erc_721(
    wallet_tokens_model,
    contract_table,
    source_id
) %}

-- ToDo ref on contracts_by_address + whitelist (??)

select distinct wallet, token_address
from {{ wallet_tokens_model }} wt
where 1=1
    and wt.wallet not in {{ source(source_id, 'blacklisted_wallets_common') }}
    and wt.wallet NOT IN ( select c.address from {{ contract_table }} c )
    AND token_address IN (
        select collection_contract
        from {{ source(source_id, 'input_claimed_audiences') }}
        where is_fungible = 0
    )

{% endmacro %}