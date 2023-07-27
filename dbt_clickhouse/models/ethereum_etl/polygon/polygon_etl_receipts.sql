{{ config(order_by='(block_number)', engine='MergeTree()', materialized='incremental') }}

SELECT
    transaction_hash,
    transaction_index,
    block_hash,
    block_number,
    cumulative_gas_used,
    gas_used,
    contract_address,
    root,
    status,
    effective_gas_price
FROM {{ source('buf_raw_data_polygon', 'buf_polygon_etl_receipts') }}
WHERE 1=1
{% if is_incremental() %}
    and block_number > (SELECT max(block_number) FROM {{ this }})
{% endif %}