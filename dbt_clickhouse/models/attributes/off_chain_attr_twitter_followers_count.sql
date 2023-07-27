{{
    config(
        materialized = 'table',
        engine = 'MergeTree()',
        order_by = 'address',
        tags = ['attr', 'off_chain', 'attr_twitter_followers_count']
    )
}}

SELECT
    address, twitter_followers_count,
    now() AS updated
FROM {{ ref('off_chain_twitters_meta') }}