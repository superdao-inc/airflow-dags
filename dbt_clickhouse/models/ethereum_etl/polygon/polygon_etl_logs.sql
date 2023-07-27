{{ config(order_by='(block_number)', engine='MergeTree()', materialized='incremental') }}

SELECT
    log_index,
    transaction_hash,
    transaction_index,
    block_hash,
    block_number,
    address,
    data,
    topics
FROM {{ source('buf_raw_data_polygon', 'buf_polygon_etl_logs') }}
WHERE 1=1
{% if is_incremental() %}
    and block_number > (SELECT max(block_number) FROM {{ this }})
{% endif %}