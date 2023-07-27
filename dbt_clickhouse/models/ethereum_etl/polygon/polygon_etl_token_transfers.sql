{{ config(order_by='(block_number)', engine='MergeTree()', materialized='incremental') }}

SELECT
    token_address,
    from_address,
    to_address,
    value,
    transaction_hash,
    log_index,
    block_number
FROM {{ source('buf_raw_data_polygon', 'buf_polygon_etl_token_transfers') }}
WHERE 1=1
{% if is_incremental() %}
    and block_number > (SELECT max(block_number) FROM {{ this }})
{% endif %}