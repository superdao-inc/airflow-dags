{{ config(order_by='(block_number)', engine='MergeTree()', materialized='incremental') }}

SELECT
    block_number,
    transaction_hash,
    transaction_index,
    from_address,
    to_address,
    value,
    input,
    output,
    trace_type,
    call_type,
    reward_type,
    gas,
    gas_used,
    subtraces,
    trace_address,
    error,
    status,
    trace_id
FROM {{ source('buf_raw_data_polygon', 'buf_polygon_etl_traces') }}
WHERE 1=1
{% if is_incremental() %}
    and block_number > (SELECT max(block_number) FROM {{ this }})
{% endif %}