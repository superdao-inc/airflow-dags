{% macro stablecoins_transfers_transactions(
  transfers_table,
  blocks_table,
  for_last_n_days
) %}


with (
  {{ block_number_by_days_ago(blocks_table, for_last_n_days) }}
) as a_time_ago,

(
  '0xc2132d05d31c914a87c6611c10748aeb04b58e8f', -- polygon usdc
  '0x2791bca1f2de4661ed88a30c99a7a9449aa84174' ,-- polygon usdt
  '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48', -- eth usdc
  '0xdac17f958d2ee523a2206206994597c13d831ec7'  -- eth usdt
) as contracts

select
  distinct transfers.transaction_hash as tx_hash
from {{ transfers_table }} transfers
where
  token_address in contracts
  and transfers.block_number >= a_time_ago

{% endmacro %}