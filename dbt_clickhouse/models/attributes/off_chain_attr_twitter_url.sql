{{
    config(
        materialized = 'table',
        engine = 'MergeTree()',
        order_by = 'address',
        tags = ['attr', 'off_chain', 'attr_twitter_url']
    )
}}

SELECT
    address, twitter_url,
    now() AS updated
FROM {{ ref('off_chain_twitters_meta') }}