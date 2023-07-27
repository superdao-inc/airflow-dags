{{
    config(
        materialized = "table",
        engine = 'MergeTree()',
        order_by = 'address',
    )
}}

WITH
(
    'elonmusk',
    'fendi',
    'senwarren',
    'cz_binance',
    'snoopdogg',
    'beberexha',
    'wef',
    'thetanmay',
    'donjazzy',
    'sbf_ftx',
    'nba',
    'manuginobili',
    'jul',
    'yg',
    'bjnovak',
    'nvtcommunity',
    'suiecosystem'
) AS banned_twitter_usernames,

('hardcoded', 'opensea') AS trusted_sources,

base_query AS (
    SELECT *
    FROM off_chain.wallets_meta
    WHERE twitter_username <> ''
        AND twitter_username IS NOT NULL
        AND lower(twitter_username) NOT IN banned_twitter_usernames
),

trusted_query AS (
    SELECT *
    FROM (
        SELECT
            *,
            wallet_address as address,
            multiIf(
                source = 'hardcoded', 1,
                source = 'opensea', 2,
                NULL
            ) as source_priority,
            row_number() OVER wndw AS priority
        FROM base_query
        WHERE source IN trusted_sources
            AND source_priority IS NOT NULL
        WINDOW wndw AS (
            PARTITION BY address
            ORDER BY source_priority, twitter_name
        )
    ) as inner
    WHERE inner.priority = 1
),

untrusted_query AS (
    SELECT *
    FROM (
        SELECT
            *,
            wallet_address as address,
            multiIf(
                source = 'ens', 1,
                source = 'eth-leaderboard', 2,
                NULL
            ) as source_priority,
            row_number() OVER wndw AS priority
        FROM base_query
        WHERE source NOT IN trusted_sources
            AND source_priority IS NOT NULL
            AND address NOT IN (
                SELECT address
                FROM trusted_query
            )
            AND twitter_username NOT IN (
                SELECT twitter_username
                FROM trusted_query
            )
        WINDOW wndw AS (
            PARTITION BY address
            ORDER BY source_priority, twitter_name
        )
    ) as inner
    WHERE inner.priority = 1
)

SELECT
    address,
    twitter_url,
    twitter_avatar_url,
    twitter_username,
    twitter_followers_count,
    twitter_location,
    twitter_bio,
    source
FROM (
    SELECT *
    FROM trusted_query
    UNION ALL
    SELECT *
    FROM untrusted_query
)