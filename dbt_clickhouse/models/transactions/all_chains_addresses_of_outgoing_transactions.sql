{{
    config(
        materialized = "table",
        engine = 'MergeTree()',
        order_by = 'address'
    )
}}

select distinct address 
from {{ ref('polygon_addresses_of_outgoing_transactions') }}

union distinct

select distinct address 
from {{ ref('eth_addresses_of_outgoing_transactions') }}