{{ config(order_by='(block_number)', engine='MergeTree()', materialized='incremental') }}

SELECT
    hash,
    nonce,
    block_hash,
    block_number,
    transaction_index,
    from_address,
    to_address,
    value,
    gas,
    gas_price,
    input,
    block_timestamp,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    transaction_type
FROM {{ source('buf_raw_data_polygon', 'buf_polygon_etl_transactions') }}
WHERE 1=1
{% if is_incremental() %}
    and block_number > (SELECT max(block_number) FROM {{ this }})
{% endif %}