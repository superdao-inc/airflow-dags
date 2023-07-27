{{
    config(
        materialized = "table",
        engine = 'MergeTree()',
        order_by = 'address',
        tags = ['attr_labels_polygon', 'attr_labels']
    )
}}


SELECT
    address,
    groupUniqArray(label) AS labels,
    now() AS updated
FROM (
    SELECT address, 'whale' AS label FROM {{ ref('tmp_polygon_label_whale') }}
    UNION ALL
    SELECT address, 'voter' AS label FROM {{ ref('tmp_polygon_label_voter') }}
    UNION ALL
    SELECT address, 'influencer' AS label FROM {{ ref('tmp_polygon_label_influencer') }}
    UNION ALL
    SELECT address, 'hunter' AS label FROM {{ ref('tmp_all_chains_nft_hunter') }}
    UNION ALL
    SELECT address, 'nonhuman' AS label FROM {{ ref('tmp_polygon_nonhuman_addresses') }}
    UNION ALL
    SELECT address, 'audience:culture:art' AS label FROM {{ ref('tmp_polygon_label_culture_art') }}
    UNION ALL
    SELECT address, 'audience:culture:fashion' AS label FROM {{ ref('tmp_polygon_label_culture_fashion') }}
    UNION ALL
    SELECT address, 'audience:culture:music' AS label FROM {{ ref('tmp_polygon_label_culture_music') }}
    UNION ALL
    SELECT address, 'audience:culture:luxury' AS label FROM {{ ref('tmp_polygon_label_culture_luxury') }}
    UNION ALL
    SELECT address, 'audience:defi' AS label FROM {{ ref('tmp_polygon_label_defi') }}
    UNION ALL
    SELECT address, 'audience:developers' AS label FROM {{ ref('tmp_polygon_label_developers') }}
    UNION ALL
    SELECT address, 'audience:donor' AS label FROM {{ ref('tmp_polygon_label_donor') }}
    UNION ALL
    SELECT address, 'audience:early_adopters' AS label FROM {{ ref('tmp_polygon_label_early_adopters') }}
    UNION ALL
    SELECT address, 'audience:gaming' AS label FROM {{ ref('tmp_polygon_label_gaming') }}
    UNION ALL
    SELECT address, 'audience:investor' AS label FROM {{ ref('tmp_polygon_label_investor') }}
    UNION ALL
    SELECT address, 'audience:professional' AS label FROM {{ ref('tmp_polygon_label_professional') }}
    UNION ALL
    SELECT address, 'nft_trader' AS label FROM {{ ref('tmp_all_chains_nft_trader') }}
    UNION ALL
    SELECT address, 'passive' AS label FROM {{ ref('tmp_all_chains_passive') }}
    UNION ALL
    SELECT address, 'zombie' AS label FROM {{ ref('tmp_all_chains_zombie') }}
)
GROUP BY address
