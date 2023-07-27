CREATE TABLE raw_data_ethereum.ethereum_etl_traces
(
`block_number`				Int256,
`transaction_hash`			String,
`transaction_index`			Int256,
`from_address`				String,
`to_address`				String,
`value`						Float64,
`input`						String,
`output`					String,
`trace_type`				String,
`call_type`					String,
`reward_type`				String,
`gas`						Int256,
`gas_used`					Int256,
`subtraces`					Int256,
`trace_address`				String,
`error`						String,
`status`					Int256,
`trace_id`					String
)
ENGINE = MergeTree
ORDER BY block_number
SETTINGS index_granularity = 8192;