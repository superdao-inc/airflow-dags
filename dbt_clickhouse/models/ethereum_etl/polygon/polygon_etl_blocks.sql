{{ config(order_by='(number)', engine='MergeTree()', materialized='incremental') }}

SELECT
    number,
    hash,
    parent_hash,
    nonce,
    sha3_uncles,
    logs_bloom,
    transactions_root,
    state_root,
    receipts_root,
    miner,
    difficulty,
    total_difficulty,
    size,
    extra_data,
    gas_limit,
    gas_used,
    timestamp,
    transaction_count,
    base_fee_per_gas
FROM {{ source('buf_raw_data_polygon', 'buf_polygon_etl_blocks') }}
WHERE 1=1
{% if is_incremental() %}
    and number > (SELECT max(number) FROM {{ this }})
{% endif %}