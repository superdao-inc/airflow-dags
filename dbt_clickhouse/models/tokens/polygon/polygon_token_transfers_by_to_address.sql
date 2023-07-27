{{
   config(
        materialized = "incremental",
        engine = 'MergeTree()',
        order_by = "(to_address, token_address, block_number, log_index)",
        incremental_strategy = "append",
        partition_by = "intDiv(block_number,2000000)",
        tags = ['token_transfers_by_address_polygon']
   )
}}

select *
from {{ source('polygon_raw_data', 'token_transfers') }}

{% if is_incremental() %}

where block_number > (select max(block_number) from {{this}} )

{% endif %}