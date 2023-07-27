{{
    config(
        materialized = 'table',
        engine = 'MergeTree()',
        order_by = 'address',
        tags = ['attr', 'off_chain', 'attr_twitter_bio']
    )
}}

SELECT
    address, twitter_bio,
    now() AS updated
FROM {{ ref('off_chain_twitters_meta') }}