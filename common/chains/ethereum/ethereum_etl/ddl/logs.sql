CREATE TABLE raw_data_ethereum.ethereum_etl_logs
(
`log_index`						Int256,
`transaction_hash`				String,
`transaction_index`				Int256,
`block_hash`					String,
`block_number`					Int256,
`address`						String,
`data`							String,
`topics`						String
)
ENGINE = MergeTree
ORDER BY block_number
SETTINGS index_granularity = 8192;