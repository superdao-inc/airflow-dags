{{
    config(
        materialized = "table",
        engine = 'MergeTree()',
        order_by = 'address'
    )
}}


select wt.wallet as address
    , countDistinct(wt.token_address, wt.token_id) as nfts_count
    , 'erc-721' as contract_type
from {{ ref('polygon_wallet_tokens') }} wt
GROUP BY wt.wallet

union all
select wallet as address
    , countDistinct(wt.token_address, wt.token_id) as nfts_count
    , 'erc-1155' as contract_type
from {{ ref('polygon_wallet_tokens_erc_1155') }} wt
group by wallet