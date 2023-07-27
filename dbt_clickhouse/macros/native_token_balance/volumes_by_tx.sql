{% macro volumes_by_tx(traces_table, blocks_table, for_last_n_days)%}

with 
(
  select number
  from {{ blocks_table }}
  where timestamp < dateSub(DAY, {{ for_last_n_days }}, now())
  order by timestamp desc
  limit 1
) as a_time_ago

select 
  traces.transaction_hash as tx_hash,
  sum(traces.value) as volume,
from {{ traces_table }} traces
where 
  traces.status = 1)
  and (
    traces.call_type not in ('delegatecall', 'callcode', 'staticcall') 
    or traces.call_type is null
  )
  and traces.block_number >= a_time_ago

{% endmacro %}