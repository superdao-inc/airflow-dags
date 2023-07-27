{{
    config(
        materialized = "table",
        engine = 'MergeTree()',
        order_by = 'address',
        tags = ['attr_labels_prerequisites']
    )
}}

WITH tx_in AS (
            SELECT
                to_address AS address,
                 count(1) AS num
            FROM {{source('polygon_data', 'transactions_by_to_address')}}
            GROUP BY to_address),
often_con AS (
            SELECT
            	address,
            	sum(num) as num_out, -- это тоже самое что сделать группировку по from_address и count(1)
            	max(num) / sum(num) AS popular_ratio_out
            FROM (
                    SELECT
                        from_address AS address,
                        count(1) AS num
                    FROM {{source('polygon_data', 'transactions_by_from_address')}}
                    GROUP BY from_address, to_address
                    ORDER BY address ASC, num DESC
            ) AS often_from
            GROUP BY often_from.address
            HAVING popular_ratio_out > 0.8 and num_out > 3000 -- здесь я объединил 2 условия, т.к. дальше использовался INNER JOIN это одно и то же
        ),
early_adopters AS (
            SELECT address
            FROM {{ ref('tmp_polygon_label_early_adopters') }}
),
with_twitters AS (
            SELECT address
            FROM {{ source('off_chain', 'attr_twitter_username') }}
),
subquery AS (
            SELECT
                nft.address AS address
            FROM {{ref('polygon_attr_nfts_count')}} AS nft
                INNER JOIN tx_in ON nft.address = tx_in.address
                INNER JOIN often_con ON nft.address = often_con.address
            WHERE
                nft.nfts_count NOT BETWEEN 5 AND 300
                AND often_con.num_out/tx_in.num > 10
                AND address NOT IN early_adopters
                AND address NOT IN with_twitters
        )

SELECT address
FROM subquery
