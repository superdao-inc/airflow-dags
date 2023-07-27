{{ config(order_by='(block_number)', engine='MergeTree()', materialized='incremental') }}

SELECT
    address,
    bytecode,
    function_sighashes,
    is_erc20,
    is_erc721,
    block_number
FROM {{ source('buf_raw_data_polygon', 'buf_polygon_etl_contracts') }}
WHERE 1=1
{% if is_incremental() %}
    and block_number > (SELECT max(block_number) FROM {{ this }})
{% endif %}