CREATE TABLE raw_data_ethereum.ethereum_etl_blocks
(
`number`				Int256,
`hash`					String,
`parent_hash`			String,
`nonce`					String,
`sha3_uncles`			String,
`logs_bloom`			String,
`transactions_root`		String,
`state_root`			String,
`receipts_root`			String,
`miner`					String,
`difficulty`			Float64,
`total_difficulty`		Float64,
`size`					Int256,
`extra_data`			String,
`gas_limit`				Int256,
`gas_used`				Int256,
`timestamp`				Int256,
`transaction_count`		Int256,
`base_fee_per_gas`		Int256
)
ENGINE = MergeTree
ORDER BY number
SETTINGS index_granularity = 8192;