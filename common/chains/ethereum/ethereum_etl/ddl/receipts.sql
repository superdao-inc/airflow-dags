CREATE TABLE raw_data_ethereum.ethereum_etl_receipts
(
`transaction_hash`						String,
`transaction_index`						Int256,
`block_hash`							String,
`block_number`							Int256,
`cumulative_gas_used`					Int256,
`gas_used`								Int256,
`contract_address`						String,
`root`									String,
`status`								Int256,
`effective_gas_price`					Int256
)
ENGINE = MergeTree
ORDER BY block_number
SETTINGS index_granularity = 8192;