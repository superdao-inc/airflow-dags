CREATE TABLE raw_data_ethereum.ethereum_etl_contracts
(
`address`						String,
`bytecode`						String,
`function_sighashes`			String,
`is_erc20`						String,
`is_erc721`						String,
`block_number`					Int256
)
ENGINE = MergeTree
ORDER BY block_number
SETTINGS index_granularity = 8192;