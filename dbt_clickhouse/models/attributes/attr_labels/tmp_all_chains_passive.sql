{{
    config(
        materialized = "table",
        engine = 'MergeTree()',
        order_by = 'address',
        tags = ['attr_labels_prerequisites', 'label_passive']
    )
}}


select
  distinct address
from {{ ref('all_chains_addresses_of_incoming_transactions') }}
where 
  address not in {{ ref('all_chains_addresses_of_outgoing_transactions') }}
