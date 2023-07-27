{{
   config(
        materialized = "table",
        engine = 'MergeTree()',
        order_by = "(address)",
        tags = ['contracts_by_address']
   )
}}

select
    coalesce(manual.address, source.address, '') as address
    , coalesce(manual.is_erc20, source.is_erc20) as is_erc20
    , coalesce(manual.is_erc721, source.is_erc721) as is_erc721
from {{ source('eth_data', 'contracts_by_address') }} source
full outer join {{ source('eth_data', 'contracts_by_address_manual')}} manual
on manual.address = source.address
settings join_use_nulls = 1
