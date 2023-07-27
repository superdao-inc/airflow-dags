{{
    config(
        materialized = "table",
        engine = 'MergeTree()',
        order_by = 'address',
        tags = ['attr_labels_prerequisites', 'label_zombie']
    )
}}

with (
    {{ block_number_by_days_ago(source('eth_data', 'blocks_by_number'), 365) }}
) as a_year_ago

select
  address,
  eth_last_txs.block_number as eth_block,
  polygon_last_txs.block_number as polygon_block
from
  {{ ref('eth_addresses_of_outgoing_transactions') }} eth_last_txs
full outer join
  {{ ref('polygon_addresses_of_outgoing_transactions') }} polygon_last_txs using (address)
where
  (eth_block <= a_year_ago and polygon_block <= a_year_ago)
  or
  (polygon_block <= a_year_ago and eth_block is null)
  or
  (eth_block <= a_year_ago and polygon_block is null)
