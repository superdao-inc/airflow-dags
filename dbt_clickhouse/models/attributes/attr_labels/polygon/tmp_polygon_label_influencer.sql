{{
    config(
        materialized = "table",
        engine = 'MergeTree()',
        order_by = 'address',
        tags = ['attr_labels_polygon']
    )
}}

SELECT address
FROM {{ ref('off_chain_attr_twitter_followers_count') }}
WHERE toInt32(twitter_followers_count) > 3000