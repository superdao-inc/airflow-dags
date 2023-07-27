{% macro erc20_transfers_transactions(
  transfers_table,
  contracts_table,
  blocks_table,
  for_last_n_days
) %}


with (
  {{ block_number_by_days_ago(blocks_table, for_last_n_days) }}
) as a_time_ago

select
  distinct transfers.transaction_hash as tx_hash
from {{ transfers_table }} transfers
inner join {{ contracts_table }} contracts
  on transfers.token_address = contracts.address
  and contracts.is_erc20 = true
where
  transfers.block_number >= a_time_ago

{% endmacro %}