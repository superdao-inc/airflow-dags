CREATE TABLE raw_data_ethereum.ethereum_etl_transactions
(
`hash`						String,
`nonce`						Int256,
`block_hash`				String,
`block_number`				Int256,
`transaction_index`			Int256,
`from_address`				String,
`to_address`				String,
`value`						Float64,
`gas`						Int256,
`gas_price`					Int256,
`input`						String,
`block_timestamp`			Int256,
`max_fee_per_gas`			Int256,
`max_priority_fee_per_gas`	Int256,
`transaction_type`			Int256
)
ENGINE = MergeTree
ORDER BY block_number
SETTINGS index_granularity = 8192;